from itertools import zip_longest
from collections import OrderedDict

def template(t, kwa={}):
    #temporarily replace \$ (escape $) with BEL
    t = t.replace('\$', '\x07')
    
    #Find $...$ parterns
    d = OrderedDict(); l = ()
    ts = []; i = 0
    for n,c in enumerate(t):
        if c=='$': l+=(n,)
        if len(l)==2:
            ts.append(t[i:l[0]]); i = l[1] + 1
            d[l[0]+1] = l[1]; l = ()
    ts.append(t[i:])

    #Find pos and kwargs
    names = []
    kwargs = {}
    for k,v in d.items():
        a = t[k:v]
        if '=' in a:
            split = a.split('=')
            name = split[0]
            kwargs[name] = '='.join(split[1:])
        else:
            name = a
        names.append(name)
    
    #update kwargs
    kwargs.update(kwa)
    for name in names:
        if name not in kwargs:
            raise ValueError('{} must be specified (it has no default value)'.format(name))

    for k in kwargs.keys():
        if k not in names:
            raise ValueError('Unused kwargs: {}'.format(k))

    out = ''.join(y for x in zip_longest(ts, map(kwargs.__getitem__, names)) for y in x if y)
    return out.replace('\x07', '$')

