import os

import pytest
from jinja2 import Environment, FileSystemLoader

@pytest.fixture
def jinja_env():
    template_dir = 'path/to/templates'  # Adjust the path to your templates directory
    return Environment(loader=FileSystemLoader(template_dir))

def test_render_table(jinja_env):
    template_dir = os.path.join(os.path.dirname(__file__), 'templates')

    env_loader = Environment(loader=FileSystemLoader(template_dir))
    template = env_loader.get_template('advanced_email_alert_template.jinja')
    headers = ['Column1', 'Column2', 'Status']
    rows = [
        ['value1', 'value2', 'PASS'],
        ['value3', 'value4', 'FAIL'],
        ['value5', 'value6', 'pass'],
        ['value7', 'value8', 'fail']
    ]
    rendered = template.render(title="Test Title", headers=headers, rows=rows)

    assert '<h2>Test Title</h2>' in rendered
    assert '<th>Column1</th>' in rendered
    assert '<td>value1</td>' in rendered
    assert '<td class="fail">FAIL</td>' in rendered
    assert '<td class="fail">fail</td>' in rendered