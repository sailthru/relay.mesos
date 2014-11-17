from distutils.core import setup
try:
    from setuptools import find_packages
except ImportError:
    print ("Please install Distutils and setuptools"
           " before installing this package")
    raise

setup(
    name='relay_mesos',
    version='0.0.1',
    description=(
        'A plugin to Relay that lets you'
        ' run an arbitrary bash command as a mesos framework.'
        ' Scale number of concurrently running instances based on a metric.'
        ' Generally good for auto-scaling workers.  Similar to Marathon,'
        ' but designed for applications that fail often or need to be'
        " autoscaled using Relay's algorithm"
    ),
    long_description="Check the project homepage for details",
    keywords=['mesos', 'marathon', 'relay', 'framework'],

    author='Alex Gaudio',
    author_email='adgaudio@gmail.com',
    url='http://github.com/sailthru/relay.mesos',

    packages=find_packages(),
    include_package_data=True,
    install_requires=['relay'],

    extras_require={
        'webui': ['pyzmq'],
        'mesos': ['mesos.native', 'mesos.cli', 'mesos.interface'],
    },
    tests_require=['nose'],
    test_suite="nose.main",

    entry_points = {
        'console_scripts': [
            'relay_mesos = relay_mesos.__main__:go',
        ],
        'setuptools.installation': [
            'eggsecutable = relay_mesos.__main__:go',
        ],
    },
)
