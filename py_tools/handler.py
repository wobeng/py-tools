import importlib.util
import os


def module_handler(file, module_name, folder='sqs'):
    path = os.path.dirname(os.path.realpath(file)) + '/{}/{}.py'.format(folder, module_name)
    name = path.split('/')[-1].replace('.py', '')
    spec = importlib.util.spec_from_file_location(name, path)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m
