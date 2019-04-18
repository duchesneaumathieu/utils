import os, sys, time
from queue import Empty, Full
from multiprocessing import Queue, Value, Process

class SoftStop(Exception): pass
class HardStop(Exception): pass

def get_loop(q, is_softstop, is_hardstop, looptime):
    while True:
        if is_hardstop(): raise HardStop()
        if is_softstop():
            try: return q.get(timeout=0)
            except Empty: raise SoftStop() from None
        else:
            try: return q.get(timeout=looptime)
            except Empty: continue
                
def put_loop(q, v, is_softstop, is_hardstop, looptime):
    while True:
        if is_hardstop(): raise HardStop()
        if is_softstop():
            try: return q.put(v, timeout=0)
            except Full: raise SoftStop() from None
        else:
            try: return q.put(v, timeout=looptime)
            except Full: continue

class xiter(object):
    def __init__(self, q, is_softstop, is_hardstop, looptime):
        self.q = q
        self.is_softstop = is_softstop
        self.is_hardstop = is_hardstop
        self.looptime = looptime

    def __iter__(self):
        return self
    
    def __next__(self):
        try: item = get_loop(self.q, self.is_softstop, self.is_hardstop, self.looptime)
        except HardStop:
            msg = 'cannot iter outside of context manager (the "with" statement)'
            raise RuntimeError(msg) from None
        except SoftStop: raise StopIteration() from None
        if isinstance(item, Exception): raise item
        else: return item

def fill_loop(it, q, stop_iteration, is_hardstop, looptime):
    while True:
        if is_hardstop(): break
        try: v = next(it)
        except StopIteration:
            stop_iteration.value = 1
            break
        except Exception as e: v = e
        try: put_loop(q, v, lambda: False, is_hardstop, looptime)
        except (SoftStop, HardStop): break
        
def work_loop(iq, oq, finish, is_softstop, is_hardstop, looptime):
    while True:
        if is_hardstop(): break
        try: x = get_loop(iq, is_softstop, is_hardstop, looptime)
        except (SoftStop, HardStop): break
            
        try: y = f(*x)
        except Exception as e: y = e
        
        try: put_loop(oq, y, lambda: False, is_hardstop, looptime)
        except (SoftStop, HardStop): break
    finish.value = 1

class xmap(object):
    def __init__(self, func, *iterables, nb_threads=1, looptime=1, imax=None, omax=None, assert_terminate=False):
        self.func = func
        self.iterable = zip(*iterables)
        self.nb_threads = nb_threads
        self.imax = nb_threads if imax is None else imax
        self.omax = nb_threads if omax is None else omax
        self.looptime = looptime
        self.context  = Value('i', 0)
        
        self.assert_terminate = assert_terminate
        self.threads = list()
        self.killed_threads = list()
        self.unkilled_threads = list()
        
    def __enter__(self):
        if self.context.value:
            raise RuntimeError('Already have context')
        self.context.value = 1
        is_without_context = lambda: not self.context.value
        
        iq = Queue(maxsize=self.imax)
        oq = Queue(maxsize=self.omax)
        
        stop_iteration = Value('i', 0)
        is_stop_iteration = lambda: bool(stop_iteration.value)
        workers_done = [Value('i', 0) for _ in range(self.nb_threads)]
        is_workers_done = lambda: all(w.value for w in workers_done)
        
        filler_args = (iter(self.iterable), iq, stop_iteration, is_without_context, self.looptime)
        filler = Process(name='filler', target=fill_loop, args=filler_args)
        filler.start()
        self.threads.append(filler)
        
        for n in range(self.nb_threads):
            worker_args = (iq, oq, workers_done[n], is_stop_iteration, is_without_context, self.looptime)
            worker = Process(name='worker{}'.format(n), target=work_loop, args=worker_args)
            worker.start()
            self.threads.append(worker)
        
        return xiter(oq, is_workers_done, is_without_context, self.looptime)
    
    def kill_threads(self):
        time.sleep(2*self.looptime+0.1) #let the process finish by themself
        for p in self.threads:
            if p.is_alive():
                p.terminate() #force
                time.sleep(0.1) #give some time for pid to be remove
                if p.is_alive():
                    msg = 'cannot stop this thread (pid={}), maybe it became sentient!'
                    sys.stderr.write(msg.format(p.pid))
                    self.unkilled_threads.append(p)
                else: self.killed_threads.append(p)
    
    def __exit__(self, exc_type, exc_value, tb):
        self.context.value = 0
        if self.assert_terminate: self.kill_threads()
        unkilled_threads = set(self.unkilled_threads)
        while self.threads:
            p = self.threads.pop()
            if p not in unkilled_threads: p.join()
        return False #reraise if errors
