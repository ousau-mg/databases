from jinja2 import Template
import subprocess

remote_url = subprocess.check_output(['git', 'remote', 'get-url', 'origin']).decode('utf-8').strip()
owner_repo = remote_url.split(':')[-1].split('.')[0]
owner = owner_repo.split('/')[0]
repo = owner_repo.split('/')[1]

# Define the dynamic data
dynamic_data = {
    'owner_repo': owner_repo,
    'owner': owner,
    'repo': repo,
}

# Load the Jinja2 template
with open('dataset/README_TEMPLATE.md', 'r') as template_file:
    readme_template_content = template_file.read()

with open('dataset/CONTRIBUTING_TEMPLATE.md', 'r') as template_file:
    contributing_template_content = template_file.read()

# Create a Jinja2 template instance
readme_template = Template(readme_template_content)
contributing_template = Template(contributing_template_content)

# Render the template with dynamic data
readme_content = readme_template.render(**dynamic_data)
contributing_content = contributing_template.render(**dynamic_data)

# Write the rendered content to README.md
with open('dataset/README.md', 'w') as readme_file:
    readme_file.write(readme_content)

with open('dataset/CONTRIBUTING.md', 'w') as contributing_file:
    contributing_file.write(contributing_content)
