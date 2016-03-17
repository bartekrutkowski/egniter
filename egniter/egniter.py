import argparse
import json
import re
import sys
import ssl
import atexit

import requests
import configparser
from pyVmomi import vim
from pyVim.connect import SmartConnect, Disconnect


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


def esx_vm_get(esx, vm_name):
    try:
        vm = esx.get_vm_by_name(vm_name)
    except VIException as e:
        print("There was an error while getting the vm: %s" % e)
        return 1
    return vm


def esx_vm_configure(config_json):

    config = config_create(config_json)
    properties = []

    esx = esx_connect(esx_host, esx_user, esx_pass)
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

    esx = esx_connect(esx_host, esx_user, esx_pass)
    vm = esx_vm_get(esx, config_json['vapp_net_hostname'])

    spec = request.new_spec()
    spec.set_element_memoryMB(config_json['hw_mem_mb'])

    request.set_element_spec(spec)
    task = esx._proxy.ReconfigVM_Task(request)._returnval
    vi_task = VITask(task, esx)

    status = vi_task.wait_for_state(
        [vi_task.STATE_SUCCESS, vi_task.STATE_ERROR])
    esx.disconnect()

    esx = esx_connect(esx_host, esx_user, esx_pass)
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
        print('ERROR: %s' % vi_task.get_error_message())
    else:
        print('vApp config successful.')
    esx.disconnect()

    # iterate over disk dictionary and add any disks found
    # to the vm configuration - the dict disk number starts with 1, not 0
    # as the disk with number 0 is already inherited from the template
    if 'hw_disk_gb' in config_json:
        for disk in config_json['hw_disk_gb']:
            esx = esx_connect(esx_host, esx_user, esx_pass)
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
                print('ERROR: %s' % vi_task.get_error_message())
            else:
                print('Disk config successful.')
            esx.disconnect()

    # iterate over network adapter dictionary and add any adapters found
    # to the vm configuration
    for adapter in config_json['hw_vmnet']['adapter']:
        esx = esx_connect(esx_host, esx_user, esx_pass)
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
            print('ERROR: %s' % vi_task.get_error_message())
        else:
            print('Network adapter config successful.')
        esx.disconnect()


def esx_vm_destroy(vm_name):
    esx = esx_connect(esx_host, esx_user, esx_pass)
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
            print('VM has been deleted.')
            return
        print('VM not found, no need to delete anything.')
        return
    except VIException as e:
        print('There were issues while getting vm: %s' % e)
        return


def launch_vm(json_file):
    config_json = json_read(json_file)
    #esx_vm_destroy(config_json['vapp_net_hostname'])
    esx = esx_connect(esx_host, esx_user, esx_pass)
    src_vm = esx_vm_get(esx, config_json['hw_template'])
    resourcepool = esx_rp_get(esx, config_json['hw_resourcepool'])
    datastore = esx_ds_get(esx, config_json['hw_datastore'])
    dst_vm = src_vm.clone(
        name=config_json['vapp_net_hostname'],
        resourcepool=resourcepool,
        datastore=datastore,
        power_on=False)
    esx_vm_configure(config_json)
    dst_vm.power_on()

# newv pyvmomi functions

def get_args():
    """
    Get commandline arguments and parse them.
    """
    parser = argparse.ArgumentParser(description='ESX Igniter', prog='egniter')
    parser.add_argument('-c', '--config',
                        action="store",
                        dest="config_file",
                        required=True,
                        help='Path to Egniter config file')
    parser.add_argument('-j', '--json',
                        action="store",
                        dest="json_file",
                        required=True,
                        help='Path to JSON file with VM definition')
    parser.add_argument('-d', '--delete',
                        action="store_true",
                        dest="delete_vm",
                        help='Delete VM if it exists')
    parser.add_argument('-s', '--strict-ssl',
                        action="store_true",
                        dest="strict_ssl",
                        help='Do strict SSL cert checks')
    args = parser.parse_args()
    return args


def get_config(config_file):
    """
    Read and parse passed config file, return ESX login credentials.
    """
    config = configparser.ConfigParser()
    config.read(config_file)
    return config['esx']['host'], config['esx']['user'], config['esx']['pass']


def json_read(path):
    try:
        with open(path, 'r') as f:
            json_data = f.read()
    except:
        print("Error reading json data file: %s" % path)
        sys.exit(1)
    try:
        config_json = json.loads(json_data)
        return config_json
    except:
        print("Error loading json data into config object.")
        sys.exit(1)


def esx_make_config_spec(vm_config):
    """
    Generate and return VM hardware spec object based on passed specification dictionary.
    """

    spec = vim.vm.ConfigSpec()
    spec.memoryMB = vm_config['hw_mem_mb']
    spec.numCPUs = vm_config['hw_vcpu']
    return spec


def esx_make_disk_spec(disk_num, disk_size):
    """
    Generate and return VM disk spec object based on passed specification dictionary.
    """

    spec = vim.vm.device.VirtualDeviceSpec()
    spec.fileOperation = "create"
    spec.operation = vim.vm.device.VirtualDeviceSpec.Operation.add
    spec.device = vim.vm.device.VirtualDisk()
    spec.device.backing = vim.vm.device.VirtualDisk.FlatVer2BackingInfo()
    spec.device.backing.diskMode = 'persistent'
    spec.device.unitNumber = int(disk_num)
    spec.device.capacityInKB = int(disk_size * 1024 * 1024)
    return spec


def esx_vm_add_disk(vm, disk_spec):
    """
    Adds disk to existing vm using spec provided.
    """

    # find the controller device
    for device in vm.config.hardware.device:
        if isinstance(device, vim.vm.device.VirtualSCSIController):
            disk_spec.device.controllerKey = device.key

    spec = vim.vm.ConfigSpec()
    spec.deviceChange.append(disk_spec)

    vm.ReconfigVM_Task(spec=spec)


def esx_make_relocate_spec(esx, vm_config):
    """
    Generate and return VM relocate spec object based on passed specification dictionary.
    """

    spec = vim.vm.RelocateSpec()
    spec.datastore = esx_get_instance(esx, [vim.Datastore],
                                      vm_config['hw_datastore'])
    spec.pool = esx_get_instance(esx, [vim.ResourcePool],
                                 vm_config['hw_resource_pool'])
    return spec


def esx_make_clone_spec(esx, vm_config):
    """
    Generate and return VM clone spec object based on passed specification dictionary.
    """

    spec = vim.vm.CloneSpec()
    spec.config = esx_make_config_spec(vm_config)
    spec.location = esx_make_relocate_spec(esx, vm_config)
    return spec


def esx_clone_vm(esx, vm_config):
    """
    Clone a VM from a template/VM, datacenter_name, vm_folder, datastore_name
    cluster_name, resource_pool, and power_on are all optional.
    """

    clonespec = esx_make_clone_spec(esx, vm_config)

    template = esx_get_instance(esx, [vim.VirtualMachine],
                                vm_config['hw_template'])
    folder = esx_get_instance(esx, [vim.Folder], vm_config['hw_folder'])

    vm = esx_watch_task(template.Clone(folder=folder,
                                       name=vm_config['hw_vm_name'],
                                       spec=clonespec))
    return vm


def esx_watch_task(task):
    """
    Wait for a vCenter task to finish.
    """

    print('Executing task...')
    while True:
        if task.info.state == 'success':
            return task.info.result

        if task.info.state == 'error':
            print('Error executing task: %s' % task.info.error.msg)
            sys.exit(1)


def esx_connect(host, user, password, strict_ssl=False):
    """
    Connect to the vCenter and return the connection object.
    """

    context = ssl.SSLContext(ssl.PROTOCOL_TLSv1)
    if not strict_ssl:
        requests.packages.urllib3.disable_warnings()
        context.verify_mode = ssl.CERT_NONE
    try:
        si = SmartConnect(host=host,
                          user=user,
                          pwd=password,
                          sslContext=context)
        esx = si.RetrieveContent()
    except Exception as e:
        print("Error while connecting to esx: %s" % e)
        sys.exit(1)
    return esx


def esx_get_instance(esx, instance_type, instance_name):
    """
    Return an instance by name, or None if it's not found.
    """

    container = esx.viewManager.CreateContainerView(esx.rootFolder,
                                                    instance_type, True)
    for obj in container.view:
        if obj.name == instance_name:
            return obj
    return None


if __name__ == '__main__':
    """
    Let's get this party started!
    """

    args = get_args()
    esx_host, esx_user, esx_pass = get_config(args.config_file)

    esx = esx_connect(esx_host, esx_user, esx_pass, args.strict_ssl)
    atexit.register(Disconnect, esx)

    vm_conf = json_read(args.json_file)
    vm = esx_clone_vm(esx, vm_conf)

    for disk in vm_conf['hw_disk_gb']:
        disk_spec = esx_make_disk_spec(disk, vm_conf['hw_disk_gb'][disk])
        esx_vm_add_disk(vm, disk_spec)
