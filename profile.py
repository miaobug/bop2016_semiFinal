# -*- coding: utf-8 -*-
hotshotProfilers = {}
# def hotshotit(func):
#     def wrapper(*args, **kw):
#         import hotshot
#         global hotshotProfilers
#         prof_name = func.func_name+".prof"
#         profiler = hotshotProfilers.get(prof_name)
#         if profiler is None:
#             profiler = hotshot.Profile(prof_name)
#             hotshotProfilers[prof_name] = profiler
#         return profiler.runcall(func, *args, **kw)
#     return wrapper


def hotshotit(func):
    return func

import hotshot
# import hotshot.stats
# stats = hotshot.stats.load('query.prof')
# stats.strip_dirs()
# stats.sort_stats('time', 'calls')
# stats.print_stats()