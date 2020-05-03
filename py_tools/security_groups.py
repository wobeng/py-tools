import boto3

client = boto3.client('ec2')
ec2 = boto3.resource('ec2')


def get_rules(name):
    rules = {
        'adds': [],
        'removes': [],
    }
    for sg in client.describe_security_groups()['SecurityGroups']:
        if sg['GroupName'] != 'default':
            rules['adds'].append({
                'id': sg['GroupId']
            })
            for ip_permission in sg['IpPermissions']:
                for ip_range in ip_permission['IpRanges']:
                    if ip_range.get('Description', '') == name:
                        rules['removes'].append({
                            'IpProtocol': ip_permission['IpProtocol'],
                            'CidrIp': ip_range['CidrIp'],
                            'FromPort': ip_permission['FromPort'],
                            'ToPort': ip_permission['ToPort'],
                            'id': sg['GroupId']
                        })
    return rules


def remove_rules(removes):
    for rule in removes:
        security_group = ec2.SecurityGroup(rule.pop('id'))
        security_group.revoke_ingress(**rule)


def add_rules(name, adds, protocol, new_ip, port):
    for rule in adds:
        security_group = ec2.SecurityGroup(rule.pop('id'))
        rule = {
            'IpProtocol': protocol,
            'FromPort': port,
            'ToPort': port,
            'IpRanges': [
                {
                    'CidrIp': new_ip + '/32',
                    'Description': name
                },
            ],
        }
        security_group.authorize_ingress(
            IpPermissions=[rule],
        )


def modify_rule(name, protocol, ip, port):
    name += '+{}'.format(port)
    rules = get_rules(name)
    remove_rules(rules['removes'])
    add_rules(name, rules['adds'], protocol, ip, port)
