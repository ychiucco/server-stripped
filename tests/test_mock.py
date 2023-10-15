from devtools import debug

def test_project_get(client):
    res = client.get(f"/api/endpoint/")
    debug(res)
    debug(res.json())