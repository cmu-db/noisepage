import psutil

PHYSICAL_CORE_NUM = psutil.cpu_count(logical = False)
