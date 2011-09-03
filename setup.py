##
## packaging script with jip

try:
    from jip.dist import setup
except ImportError:
    try:
        from setuptools import setup
    except:
        from distutils import setup

requires_java = {
    'dependencies':[
        ('org.apache.pig','pig','0.8.3'),
        ('org.apache.hadoop', 'hadoop-core', '0.20.203.0'),
        ('log4j', 'log4j', '1.2.16')
    ],
    'exclusions':[
        ('ant', 'ant'),
        ('org.apache.ant', 'ant'),
        ('junit', 'junit')
    ]
}

def get_squealer_version():
    import re
    root_module_file = open('./squealer/__init__.py')
    for line in root_module_file.readlines():
        if line.startswith('__version__'):
            matches = re.findall("__version__ = '(.*)'", line)
            if matches:
                return matches[0]
            else:
                return 'UNKNOWN_VERSION'

setup(
    name='squealer',
    author='markroddy',
    version = get_squealer_version(),
    description='Squealer is a Jython library for unit testing Apache Pig scripts.',
    install_requires=['jip'],
    requires_java=requires_java,
    packages=['squealer']
)

