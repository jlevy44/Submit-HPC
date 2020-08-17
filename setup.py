from setuptools import setup
with open('README.md','r', encoding='utf-8') as f:
      long_description = f.read()
setup(name='submit_hpc',
      version='0.1.2',
      description='Collection of growing job submission scripts, not to replace workflow specifications.',
      url='https://github.com/jlevy44/Submit-HPC',
      author='Joshua Levy',
      author_email='joshualevy44@berkeley.edu',
      license='MIT',
      scripts=[],
      entry_points={
            'console_scripts':['submit-job=submit_hpc.job_runner:job']
      },
      long_description=long_description,
      long_description_content_type='text/markdown',
      packages=['submit_hpc'],
      install_requires=['click'])
