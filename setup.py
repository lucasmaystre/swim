#!/usr/bin/env python
"""Setup script."""


from distutils.core import setup


setup(
    name='swim',
    version='0.1',
    description='A simple, no-frills crawler.',
    author='Lucas Maystre',
    author_email='lucas@maystre.ch',
    url='http://lucas.maytstre.ch',
    py_modules=['swim'],
    requires=['futures', 'storm', 'requests'],
    license='MIT',
)
