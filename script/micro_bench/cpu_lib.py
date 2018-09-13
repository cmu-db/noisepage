#!/usr/bin/env python

"""
cpu - logical cpu, i.e. one execution "thread"
core - a single cpu core, which may contain multiple logical CPUs
       i.e. hyperthreads.
socket - a physical cpu. May contain multiple cores.
numa - cpus are associated with a numa node. 
       when there is more than one numa node, cpu cost for accessing the
       local numa node is less than access cost for a remote numa node.

Background info:
The number of NUMA nodes does not always equal the number of
sockets. For example, an AMD Threadripper 1950X has 1 socket and 2
NUMA nodes while a dual Intel Xeon E5310 system can show 2 sockets and
1 NUMA node.
"""

import re
import subprocess

class CPUBase(object):
    _shared_state = {}    
    def __init__(self):
        self.__dict__ = self._shared_state
        return

class CPUSocket(object):
    def __init__(self, cpu_dict):
        # aka physical id
        attr_list = ["cpu", "core", "socket", "node"]
        for attr in attr_list:
            setattr(self, attr + "_id", cpu_dict[attr])

        self.cpu_obj_list = []
        return

    def add_cpu(self, cpu_obj):
        self.cpu_obj_list.append(cpu_obj)
        return
    
class CPUCore(object):
    """ A physical cpu that may have one or more threads, i.e.
        one or more logical cpus
    """
    def __init__(self, cpu_dict):
        attr_list = ["cpu", "core", "socket", "node"]
        for attr in attr_list:
            setattr(self, attr + "_id", cpu_dict[attr])
        
        # list of cpus in this core
        self.cpu_obj_list = []
        return

    def add_cpu(self, cpu_obj):
        self.cpu_obj_list.append(cpu_obj)
        return

class CPU(object):
    """ A logical cpu """
    def __init__(self, cpu_dict):
        attr_list = ["cpu", "core", "socket", "node"]
        for attr in attr_list:
            setattr(self, attr + "_id", cpu_dict[attr])

        # available
        self.free = True
        return

    def free(self):
        assert(not self.free)
        self.free = True
        return

    def get_cpu_id(self):
        return self.cpu_id

    def is_free(self):
        return self.free

    def reserve(self):
        assert(self.free)
        self.free = False
        return

class NumaNode(object):
    """ Container for numa nodes """
    def __init__(self, cpu_dict):
        attr_list = ["cpu", "core", "socket", "node"]
        for attr in attr_list:
            setattr(self, attr + "_id", cpu_dict[attr])

            self.cpu_obj_list = []
        return

    def add_cpu(self, cpu_obj):
        self.cpu_obj_list.append(cpu_obj)        
        return

    def get_cpu_list(self):
        return self.cpu_obj_list

class CPUAllocator(object):
    def __init__(self):

        # cpu objects, keyed by cpu id
        self.cpu_dict = {}

        # numa node objects, keyed by numa id
        self.numa_node_dict = {}

        # cpu core objects, keyed by core id
        self.core_dict = {}

        # cpu socket objects, keyed by socket id
        self.socket_dict = {}

        # available and reserved cpus
        # set self.possible_cpus
        self._init_non_isolcpus()
        
        # set self.reserved_cpus
        self._init_isolcpus()
        
        # get info from lscpu
        ls_cpu = LsCPU()
        cpu_dict_list = ls_cpu.get_lscpu_info()
        for cpu_dict in cpu_dict_list:
            self.add_cpu(cpu_dict)
            
            # add to core
            self.add_cpu_to_core(cpu_dict)
            
            # add to socket
            
            # add to numa
            self.add_cpu_to_numa(cpu_dict)
            pass
        return

    def add_cpu(self, cpu_dict):
        cpu_id = cpu_dict["cpu"]
        if self.cpu_dict.has_key(cpu_id):
            return
        
        cpu_obj = CPU(cpu_dict)
        self.cpu_dict[cpu_id] = cpu_obj
        return

    def add_cpu_to_core(self, cpu_dict):
        core_id = cpu_dict["core"]
        cpu_id = cpu_dict["cpu"]
        # cpu must already have been added, i.e. add_cpu has been called
        assert(self.cpu_dict.has_key(cpu_id))
        cpu_obj = self.cpu_dict[cpu_id]
        
        if not self.core_dict.has_key(core_id):
            # no core object, create it
            core_obj = CPUCore(cpu_dict)
            self.core_dict[core_id] = core_obj
        else:
            core_obj = self.core_dict[core_id]
            
        # add cpu to core
        core_obj.add_cpu(cpu_obj)
        return

    def add_cpu_to_numa(self, cpu_dict):
        node_id = cpu_dict["node"]
        cpu_id = cpu_dict["cpu"]
        
        # cpu must already have been added, i.e. add_cpu has been called
        assert(self.cpu_dict.has_key(cpu_id))
        cpu_obj = self.cpu_dict[cpu_id]

        if not self.numa_node_dict.has_key(node_id):
            # no numa node object, create it
            numa_obj = NumaNode(cpu_dict)
            self.numa_node_dict[node_id] = numa_obj
        else:
            numa_obj = self.numa_node_dict[node_id]

        # add cpu to numa node
        numa_obj.add_cpu(cpu_obj)
        return

    """
    def add_cpu_to_socket(self, cpu_dict):
        # add cpu to socket
        return
    """
    
    def get_cpus(self):
        return

    def get_numas(self):
        return

    def get_numa_ids(self):
        ret_list = self.numa_node_dict.keys()
        ret_list.sort()
        return ret_list

    def get_sockets(self):
        return

    # ----
    # execution helper methods
    # ----
    
    def free_cpus(self, cpu_list):
        """ Free the cpus in the list
            cpu_list: 
        """
        for cpu_id in cpu_list:
            self._free_cpu(cpu_id)
        return
    
    def get_n_cpus(self, num_cpus, low=True):
        """ Reserve and return the specified number of cpus
            num_cpus: number of desired cpus
            low: if true, return lowest numbered free cpus, otherwise highest numbered
            returns: list of cpu_ids
        """
        cpu_list = list(self.possible_cpus)
        cpu_list.sort()
        if not low:
            cpu_list.reverse()

        # filter down to free cpus
        """
        free_cpu_list = []
        for cpu_id in cpu_list:
            if self.cpu_dict[cpu_id].is_free():
                free_cpu_list.append(cpu_id)
        """
        free_cpu_list = self._filter_cpus_free(cpu_list)

        ret_list = free_cpu_list[:num_cpus]
        if len(ret_list) != num_cpus:
            return []

        for cpu_id in ret_list:
            self._reserve_cpu(cpu_id)
        return ret_list

    def get_numa_by_id(self, numa_id):
        return self.numa_node_dict[numa_id]

    def reserve_cpus(self, cpu_list):
        """ reserve the specified cpus 
            cpu_list - numeric list of cpus to reserve
        """
        return


    # ----
    # internal methods
    # ----

    def _filter_cpus_free(self, cpu_list, free=True):
        """ Filter cpu list 
            free: True, return only free CPUs
                  False, return reserved CPUs
        """
        
        ret_list = []
        for cpu_id in cpu_list:
            cpu_obj = self.cpu_dict[cpu_id]

            # honor free or not free
            if cpu_obj.is_free() == free:
                ret_list.append(cpu_id)
        return ret_list

    def _filter_cpus_no_ht(self, cpu_list):
        """ Filter cpu list to exclude HT peers 
            returns integer cpu list
        """
        ret_list = []
        core_id_set = set()
        for cpu_id in cpu_list:
            cpu_obj = self.cpu_dict[cpu_id]
            core_id = cpu_obj.core_id
            if core_id in core_id_set:
                # already have a cpu from this core
                continue
            core_id_set.add(core_id)
            ret_list.append(cpu_id)
        return ret_list

    def _free_cpu(self, cpu_id):
        """ free a reserved cpu """
        assert(self.cpu_dict.has_key(cpu_id))
        cpu_obj = self.cpu_dict[cpu_id]
        
        cpu_obj.free()
        return
        
        return

    def _init_non_isolcpus(self):
        # /sys/devices/system/cpu/possible
        self.possible_cpus = self._parse_dev_system_cpu_spec(
            self._read_sys_item("/sys/devices/system/cpu/possible")[0])

        # ?? this may include isolated cpus?
        return

    def _init_isolcpus(self):
        """ The set of isolated cpus """
        # /sys/devices/system/cpu/isolated
        self.reserved_cpus = self._parse_dev_system_cpu_spec(
            self._read_sys_item("/sys/devices/system/cpu/isolated")[0])
        return

    def _parse_dev_system_cpu_spec(self, cpu_spec):
        """ Parse a cpu spec, as provided by /sys/devices/system/cpu/xxx
            and turn it into a set.

            returns a set of integers 
        """
        ret_set =  set()
        cpu_spec = cpu_spec.strip()
        if cpu_spec == "":
            return ret_set

        parts = cpu_spec.split(",")
        for part in parts:
            if part.find("-") == -1:
                # just an integer
                cpu = int(part)
                ret_set.add(cpu)
                continue

            # is of the form n-m
            int_range = map(int, part.split("-"))
            for i in range(int_range[0], int_range[1]+1):
                ret_set.add(i)
        return ret_set
    
    def _read_sys_item(self, path):
        """ Return contents of a file """
        with open(path) as fh:
            return fh.readlines()

    def _reserve_cpu(self, cpu_id):
        """ Mark a cpu as reserved """
        assert(self.cpu_dict.has_key(cpu_id))
        cpu_obj = self.cpu_dict[cpu_id]
        
        cpu_obj.reserve()
        return

# ----
# helper classes
# ----

"""
The number of NUMA nodes does not always equal the number of sockets.
For example, an AMD Threadripper 1950X has 1 socket and 2 NUMA nodes
while a dual Intel Xeon E5310 system can show 2 sockets and 1 NUMA node.
"""    

class LsCPU(object):
    """ Get cpu information from lscpu and make available as a dictionary """
    def __init__(self):
        self.ret_attr_list = ["cpu", "core", "socket", "node"]
        return

    def get_lscpu_info(self):
        ret_dict_list = []

        cmd = "lscpu --parse=" + ",".join(self.ret_attr_list)
        out_str = subprocess.check_output(cmd, shell=True)

        # convert the output string into lines
        lines = out_str.splitlines()
        for line in lines:
            if not line:
                # empty line, ignore
                continue

            if (len(line) >= 1) and (line[0] == "#"):
                # it is a comment line, ignore
                continue

            cpu_dict = {}
            # one of these lines for each cpu
            int_parts = map(int, line.split(","))
            int_parts.reverse()
            for attr in self.ret_attr_list:
                cpu_dict[attr] = int_parts.pop()

            ret_dict_list.append(cpu_dict)

        # returns list of dictionaries. 
        return ret_dict_list

if __name__ == "__main__":
    h = LsCPU()
    print h.get_lscpu_info()

    hem = CPUAllocator()
        
    print hem.possible_cpus

    cpu_list = hem.get_n_cpus(2, low=False)
    print cpu_list

    numa_id_list = hem.get_numa_ids()
    print numa_id_list

    cpu_a = CPUAllocator()
    # get the highest number numa node
    numa_id = cpu_a.get_numa_ids()[-1]
    numa_obj = cpu_a.get_numa_by_id(numa_id)
    cpu_obj_list = numa_obj.get_cpu_list()
    
    cpu_id_list = []
    for cpu in cpu_obj_list:
        if not cpu.is_free():
            continue
        cpu_id_list.append(cpu.get_cpu_id())
    print cpu_id_list
