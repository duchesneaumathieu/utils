import os, torch

class States(object):
    def __init__(self, **attr):
        self.attr = attr
        
    def get_state(self, item):
        try: return item.state_dict()
        except AttributeError: return item
        
    def set_state(self, name, item):
        try: self.name.load_state_dict(item)
        except AttributeError: self.__setattr__(name, item)
        
    def get_states(self):
        return {k:self.get_state(v) for k,v in self.attr.items()}
    
    def set_states(self, states):
        for name, item in states.items():
            self.set_state(name, item)
    
    def save(self, path):
        states = self.get_states()
        torch.save(states, path)
        return self
        
    def load(self, path):
        if os.path.isfile(path):
            states = torch.load(path)
            self.set_states(states)
        return self
        
    def __getattr__(self, name):
        if name in self.attr: return self.attr[name]
        super().__getattribute__(name)
    
    def __setattr__(self, name, value):
        if name in {'attr', 'get_state', 'set_state', 'get_states', 'set_states', 'save', 'load'}:
            try:
                super().__getattribute__(name)
                msg = '{} is a private attribute and cannot be overwritten'
                raise ValueError(msg.format(name))
            except AttributeError: super().__setattr__(name, value)
        else: self.attr[name] = value
            
    def __repr__(self):
        return repr(self.attr)
