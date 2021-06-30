from os.path import dirname, join
import sys

BASE = dirname(dirname(dirname(__file__)))
sys.path.insert(0, join(BASE, 'build'))
