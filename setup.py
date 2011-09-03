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
        ('junit', 'junit')
    ]
}

import squealer

setup(
    name='squealer',
    author='markroddy',
    version = squealer.__version__,
    description='Squealer is a Jython library for unit testing Apache Pig scripts.',
    install_requires=['jip'],
    requires_java=requires_java,
    packages=['squealer']
)

