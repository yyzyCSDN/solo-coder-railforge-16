import unittest
from datetime import datetime,timezone,timedelta,date
from decimal import Decimal
from railforge.api import RailForgeService
from railforge.signaling.block_occupancy import Occupancy
from railforge.consist.integrity import WagonLink,validate
from railforge.braking.brake_percentage import Vehicle,brake_percentage
from railforge.audit.trail import Trail
from railforge.planning.train_path_engine import Section,PathRequest,build
U=timezone.utc
class PublicTests(unittest.TestCase):
    def test_health(self):
        h=RailForgeService().health();self.assertEqual(h['status'],'ok');self.assertEqual(h['runtime_dependencies'],0)
    def test_block_smoke(self):
        a=Occupancy('A','B1',datetime(2026,1,1,8,tzinfo=U),datetime(2026,1,1,9,tzinfo=U),1)
        b=Occupancy('B','B1',datetime(2026,1,1,10,tzinfo=U),datetime(2026,1,1,11,tzinfo=U),1)
        self.assertEqual(RailForgeService().block_conflicts(b,[a],timedelta(minutes=5)),[])
    def test_consist_smoke(self):
        rows=[WagonLink(1,'W1',None,'W2',True),WagonLink(2,'W2','W1',None,True)]
        self.assertEqual(validate(rows),('W1','W2'))
    def test_brake_smoke(self):
        self.assertGreater(brake_percentage([Vehicle('W',100,90,True)],0),80)
    def test_audit(self):
        t=Trail();t.append('dispatcher','create','T1');t.append('supervisor','approve','T1');self.assertTrue(t.verify())
    def test_path_engine(self):
        s={'A':Section('A',30,True,2)}
        r=[PathRequest('T',datetime(2026,1,1,8,tzinfo=U),('A',),1,10)]
        self.assertIn('T',build(r,s,[],3))
if __name__=='__main__':unittest.main()
