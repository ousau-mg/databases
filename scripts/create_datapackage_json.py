import os
from frictionless import describe

def create_datapackage_json():
    package = describe('./dataset/datapackage.yaml', type='package')
    package.to_json('./dataset/datapackage.json')

if __name__ == '__main__':
    create_datapackage_json()
