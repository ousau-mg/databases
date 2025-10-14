from frictionless import validate

def validate_package():
    report = validate('./dataset/datapackage.json')
    report.to_json('datapackage_validation.json')

if __name__ == '__main__':
    validate_package()
