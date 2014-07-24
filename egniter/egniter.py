import argparse
import json
import re
import sys

import configparser
from pysphere import VIServer, VITask, VIApiException, VIException
from pysphere.resources import VimService_services as VI


parser = argparse.ArgumentParser(description='ESX Igniter', prog='egniter')
parser.add_argument('-c', action="store", dest="config_file", required=True)
parser.add_argument('-f', action="store", dest="json_file", required=True)
parser.add_argument('-d', action="store_true", dest="delete_vm")
args = parser.parse_args()


config = configparser.ConfigParser()
config.read(args.config_file)
ESX_HOST = config['esx']['host']
ESX_USER = config['esx']['user']
ESX_PASS = config['esx']['pass']


def esx_rp_get(esx, rp_name):
    rps = esx.get_resource_pools()
    for mor, path in rps.iteritems():
        if re.match('.*%s' % rp_name, path):
            return mor
    return None


def esx_ds_get(esx, ds_name):
    datastores = esx.get_datastores()
    for mor, path in datastores.iteritems():
        if re.match('.*%s' % ds_name, path):
            return mor
    return None


def json_read(path):
    try:
        with open(path, 'r') as f:
            json_data = f.read()
    except:
        print("There were issues reading json data file: %s" % path)
        return 1
    try:
        config_json = json.loads(json_data)
        return config_json
    except:
        print("There were issues in loading json data into config object.")
        return 1


def config_create(config_json):
    counter = 1000
    vapp_properties = {'add': []}
    for k, v in config_json.iteritems():
        if 'vapp_' in k:
            counter += 1
            name = k.split('vapp_')[1]
            vapp_properties['add'].append({
                ### make VAPP_CATEGORY being configurable
                'key': counter,
                'id': name,
                'value': v,
                'category': 'VAPP_CATEGORY'
            })
        elif 'hw_vmnet' in k:
            for ns in v['dnsresolver']:
                counter += 1
                vapp_properties['add'].append({
                    'key': counter,
                    'id': 'net_dnsresolver_%s' % ns,
                    'value': v['dnsresolver'][ns],
                    'category': 'VAPP_CATEGORY'
                })
            for nic in v['adapter']:
                counter += 1
                vapp_properties['add'].append({
                    'key': counter,
                    'id': 'net_ipaddress_%s' % nic,
                    'value': v['adapter'][nic]['ipaddress'],
                    'category': 'VAPP_CATEGORY'
                })
                counter += 1
                vapp_properties['add'].append({
                    'key': counter,
                    'id': 'net_netmask_%s' % nic,
                    'value': v['adapter'][nic]['netmask'],
                    'category': 'VAPP_CATEGORY'
                })
            counter += 1
            vapp_properties['add'].append({
                'key': counter,
                'id': 'net_gateway',
                'value': v['gateway'],
                'category': 'VAPP_CATEGORY'
            })
    return vapp_properties


def esx_connect(host, user, password):
    esx = VIServer()
    try:
        esx.connect(host, user, password)
    except VIApiException, e:
        print("There was an error while connecting to esx: %s" % e)
        return 1
    return esx


def esx_vm_get(esx, vm_name):
    try:
        vm = esx.get_vm_by_name(vm_name)
    except VIException, e:
        print ("There was an error while getting the vm: %s" % e)
        return 1
    return vm


def esx_vm_configure(config_json):

    config = config_create(config_json)
    properties = []

    esx = esx_connect(ESX_HOST, ESX_USER, ESX_PASS)
    vm = esx_vm_get(esx, config_json['vapp_net_hostname'])

    request = VI.ReconfigVM_TaskRequestMsg()
    _this = request.new__this(vm._mor)
    _this.set_attribute_type(vm._mor.get_attribute_type())
    request.set_element__this(_this)

    spec = request.new_spec()
    vappconfig = spec.new_vAppConfig()

    for operation, items in config.items():
        for item in items:
            prop = vappconfig.new_property()
            prop.set_element_operation(operation)
            info = prop.new_info()
            for k, v in item.items():
                method = getattr(info, "set_element_" + k)
                method(v)
            prop.set_element_info(info)
            properties.append(prop)

    vappconfig.set_element_property(properties)
    spec.set_element_vAppConfig(vappconfig)

    request.set_element_spec(spec)
    task = esx._proxy.ReconfigVM_Task(request)._returnval
    vi_task = VITask(task, esx)

    status = vi_task.wait_for_state(
        [vi_task.STATE_SUCCESS, vi_task.STATE_ERROR])
    esx.disconnect()

    esx = esx_connect(ESX_HOST, ESX_USER, ESX_PASS)
    vm = esx_vm_get(esx, config_json['vapp_net_hostname'])

    spec = request.new_spec()
    spec.set_element_memoryMB(config_json['hw_mem_mb'])

    request.set_element_spec(spec)
    task = esx._proxy.ReconfigVM_Task(request)._returnval
    vi_task = VITask(task, esx)

    status = vi_task.wait_for_state(
        [vi_task.STATE_SUCCESS, vi_task.STATE_ERROR])
    esx.disconnect()

    esx = esx_connect(ESX_HOST, ESX_USER, ESX_PASS)
    vm = esx_vm_get(esx, config_json['vapp_net_hostname'])

    request = VI.ReconfigVM_TaskRequestMsg()
    _this = request.new__this(vm._mor)
    _this.set_attribute_type(vm._mor.get_attribute_type())
    request.set_element__this(_this)

    spec = request.new_spec()
    spec.set_element_numCoresPerSocket(config_json['hw_vcpu'])
    spec.set_element_numCPUs(config_json['hw_vcpu'])

    request.set_element_spec(spec)
    task = esx._proxy.ReconfigVM_Task(request)._returnval
    vi_task = VITask(task, esx)

    status = vi_task.wait_for_state(
        [vi_task.STATE_SUCCESS, vi_task.STATE_ERROR])
    if status == vi_task.STATE_ERROR:
        print ('ERROR: %s' % vi_task.get_error_message())
    else:
        print ('vApp config successful.')
    esx.disconnect()

    # iterate over disk dictionary and add any disks found
    # to the vm configuration - the dict disk number starts with 1, not 0
    # as the disk with number 0 is already inherited from the template
    if 'hw_disk_gb' in config_json:
        for disk in config_json['hw_disk_gb']:
            esx = esx_connect(ESX_HOST, ESX_USER, ESX_PASS)
            vm = esx_vm_get(esx, config_json['vapp_net_hostname'])

            request = VI.ReconfigVM_TaskRequestMsg()
            _this = request.new__this(vm._mor)
            _this.set_attribute_type(vm._mor.get_attribute_type())
            request.set_element__this(_this)

            spec = request.new_spec()

            dc = spec.new_deviceChange()
            dc.Operation = "add"
            dc.FileOperation = "create"

            hd = VI.ns0.VirtualDisk_Def("hd").pyclass()
            hd.Key = -100
            hd.UnitNumber = int(disk)
            hd.CapacityInKB = config_json['hw_disk_gb'][disk] * 1024 * 1024
            hd.ControllerKey = 1000

            backing = VI.ns0.VirtualDiskFlatVer2BackingInfo_Def(
                "backing").pyclass()
            backing.FileName = "%s" % vm.get_property('path').split()[0]
            backing.DiskMode = "persistent"
            backing.Split = False
            backing.WriteThrough = False
            backing.ThinProvisioned = False
            backing.EagerlyScrub = False
            hd.Backing = backing

            dc.Device = hd

            spec.DeviceChange = [dc]
            request.Spec = spec

            request.set_element_spec(spec)
            task = esx._proxy.ReconfigVM_Task(request)._returnval
            vi_task = VITask(task, esx)

            # Wait for task to finis
            status = vi_task.wait_for_state([vi_task.STATE_SUCCESS,
                                             vi_task.STATE_ERROR])
            if status == vi_task.STATE_ERROR:
                print ('ERROR: %s' % vi_task.get_error_message())
            else:
                print ('Disk config successful.')
            esx.disconnect()

    # iterate over network adapter dictionary and add any adapters found
    # to the vm configuration
    for adapter in config_json['hw_vmnet']['adapter']:
        esx = esx_connect(ESX_HOST, ESX_USER, ESX_PASS)
        vm = esx_vm_get(esx, config_json['vapp_net_hostname'])

        request = VI.ReconfigVM_TaskRequestMsg()
        _this = request.new__this(vm._mor)
        _this.set_attribute_type(vm._mor.get_attribute_type())
        request.set_element__this(_this)

        spec = request.new_spec()
        dev_change = spec.new_deviceChange()
        dev_change.set_element_operation('add')
        nic_ctlr = VI.ns0.VirtualVmxnet3_Def('nic_ctlr').pyclass()
        nic_backing = VI.ns0.VirtualEthernetCardNetworkBackingInfo_Def(
            'nic_backing').pyclass()
        nic_backing.set_element_deviceName(
            config_json['hw_vmnet']['adapter'][adapter]['label'])
        nic_ctlr.set_element_addressType('generated')
        nic_ctlr.set_element_backing(nic_backing)
        nic_ctlr.set_element_key(4)
        dev_change.set_element_device(nic_ctlr)

        spec.set_element_deviceChange([dev_change])
        request.set_element_spec(spec)
        ret = esx._proxy.ReconfigVM_Task(request)._returnval

        # Wait for the task to finish
        vi_task = VITask(ret, esx)

        status = vi_task.wait_for_state([vi_task.STATE_SUCCESS,
                                         vi_task.STATE_ERROR])
        if status == vi_task.STATE_ERROR:
            print ('ERROR: %s' % vi_task.get_error_message())
        else:
            print ('Network adapter config successful.')
        esx.disconnect()


def esx_vm_destroy(vm_name):
    esx = esx_connect(ESX_HOST, ESX_USER, ESX_PASS)
    try:
        vm = esx_vm_get(esx, vm_name)
        if not isinstance(vm, int):
            if not args.delete_vm:
                print('ERROR: I cant destroy the VM, because delete '
                      'argument was not used, exiting.')
                sys.exit(1)
            if not vm.is_powered_off():
                vm.power_off()
            vm.destroy()
            print ('VM has been deleted.')
            return
        print ('VM not found, no need to delete anything.')
        return
    except VIException, e:
        print('There were issues while getting vm: %s' % e)
        return


def launch_vm(json_file):
    config_json = json_read(json_file)
    esx_vm_destroy(config_json['vapp_net_hostname'])
    esx = esx_connect(ESX_HOST, ESX_USER, ESX_PASS)
    src_vm = esx_vm_get(esx, config_json['hw_template'])
    resourcepool = esx_rp_get(esx, config_json['hw_resourcepool'])
    datastore = esx_ds_get(esx, config_json['hw_datastore'])
    dst_vm = src_vm.clone(
        name=config_json['vapp_net_hostname'],
        resourcepool=resourcepool,
        datastore=datastore, power_on=False)
    esx_vm_configure(config_json)
    dst_vm.power_on()


def main():
    launch_vm(args.json_file)

if __name__ == '__main__':
    main()
