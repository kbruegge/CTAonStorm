"""
Event count topology
"""

from streamparse import Grouping, Topology

from bolts.wordcount import HillasBolt, RecoBolt, EmptyBolt
from spouts.words import EventSpout


class WordCount(Topology):
    word_spout = EventSpout.spec()
    hillas_bolt = HillasBolt.spec(inputs=[word_spout])
    empty = RecoBolt.spec(inputs=[hillas_bolt])
    # reco_bolt = RecoBolt.spec(inputs={hillas_bolt: Grouping.LOCAL_OR_SHUFFLE}, par=2)
