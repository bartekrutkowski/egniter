import argparse
import json
import sys
import ssl
import atexit

import requests
import configparser
from pyVmomi import vim
from pyVim.connect import SmartConnect, Disconnect


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
    parser.add_argument('-d', '--destroy',
                        action="store_true",
                        dest="destroy_vm",
                        help='Destroy VM if it exists')
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
    Generate and return VM hardware spec object based on passed dictionary.
    """

    spec = vim.vm.ConfigSpec()
    spec.memoryMB = vm_config['hw_mem_mb']
    spec.numCPUs = vm_config['hw_vcpu']
    return spec


def esx_make_disk_spec(disk_num, disk_size):
    """
    Generate and return VM disk spec object based on passed dictionary.
    """

    spec = vim.vm.device.VirtualDeviceSpec()
    spec.fileOperation = 'create'
    spec.operation = vim.vm.device.VirtualDeviceSpec.Operation.add
    spec.device = vim.vm.device.VirtualDisk()
    spec.device.backing = vim.vm.device.VirtualDisk.FlatVer2BackingInfo()
    spec.device.backing.diskMode = 'persistent'
    spec.device.unitNumber = int(disk_num) + 1
    spec.device.capacityInKB = int(disk_size) * 1024 * 1024
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

    esx_watch_task(vm.ReconfigVM_Task(spec=spec))


def esx_make_relocate_spec(esx, vm_config):
    """
    Generate and return VM relocate spec object based on passed dictionary.
    """

    spec = vim.vm.RelocateSpec()
    spec.datastore = esx_get_instance(esx, [vim.Datastore],
                                      vm_config['hw_datastore'])
    spec.pool = esx_get_instance(esx, [vim.ResourcePool],
                                 vm_config['hw_resource_pool'])
    return spec


def esx_make_clone_spec(esx, vm_config):
    """
    Generate and return VM clone spec object based on passed dictionary.
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


def esx_vm_destroy(esx, vm_name):
    """
    Destroys the vm based on the name provided.
    """

    vm = esx_get_instance(esx, [vim.VirtualMachine], vm_name)
    if not vm:
        print('No vm {} found.'.format(vm_name))
        return
    task = vm.Destroy_Task()
    esx_watch_task(task)
    return


def esx_watch_task(task):
    """
    Wait for a vCenter task to finish.
    """

    print('Executing task: {task}, {name}'.format(task=task.info.descriptionId,
                                                  name=task.info.entityName))
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

    :esx esx connection content object (from esx_connect() function)
    :instance_type vim type of instance to look for (like [vim.VirtualMachine])
    :instance_name string with the name of instance to look for
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

    if args.destroy_vm:
        esx_vm_destroy(esx, vm_conf['hw_vm_name'])

    vm = esx_clone_vm(esx, vm_conf)

    for disk in sorted(vm_conf['hw_disk_gb']):
        disk_spec = esx_make_disk_spec(disk, vm_conf['hw_disk_gb'][disk])
        esx_vm_add_disk(vm, disk_spec)
