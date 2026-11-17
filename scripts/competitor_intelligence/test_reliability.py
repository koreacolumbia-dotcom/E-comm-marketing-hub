import unittest
from reliability import PolicyCache

class Response:
    def __init__(self,status,body='',location=''):
        self.status,self.body=status,body
        self.headers={'Location':location}
    async def __aenter__(self): return self
    async def __aexit__(self,*args): return False
    async def text(self): return self.body

class Session:
    def __init__(self,*responses):
        self.responses=list(responses)
        self.urls=[]
    def get(self,url,**kwargs):
        self.urls.append(url)
        return self.responses.pop(0)

class PolicyTests(unittest.IsolatedAsyncioTestCase):
    def policy(self,*responses):
        s=Session(*responses)
        return PolicyCache(s,'CSKMarketResearchBot',lambda u:u.startswith('https://example.com/')),s
    async def test_same_host_redirect(self):
        p,s=self.policy(Response(302,location='/policy.txt'),Response(200,'User-agent: *\nDisallow: /private'))
        self.assertTrue(await p.permitted('https://example.com/shop'))
        self.assertFalse(await p.permitted('https://example.com/private'))
        self.assertEqual(len(s.urls),2)
    async def test_external_redirect_denied(self):
        p,s=self.policy(Response(302,location='https://other.example/robots.txt'))
        self.assertFalse(await p.permitted('https://example.com/shop'))
        self.assertEqual(len(s.urls),1)
    async def test_403_denied(self):
        p,s=self.policy(Response(403))
        self.assertFalse(await p.permitted('https://example.com/shop'))
        self.assertEqual(p.reason('https://example.com/shop'),'robots HTTP 403')
    async def test_404_allowed(self):
        p,s=self.policy(Response(404))
        self.assertTrue(await p.permitted('https://example.com/shop'))
    async def test_html_not_policy(self):
        p,s=self.policy(Response(200,'<html>login</html>'))
        self.assertFalse(await p.permitted('https://example.com/shop'))
