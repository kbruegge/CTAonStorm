"""
Event count topology
"""

from streamparse import Topology

from bolts.wordcount import HillasBolt, RecoBolt, HillasErrorBolt
from spouts.words import EventSpout


class CTATopology(Topology):
    word_spout = EventSpout.spec()
    hillas_bolt = HillasBolt.spec(inputs=[word_spout])
    reco_bolt = RecoBolt.spec(inputs=[hillas_bolt])

    error_bolt = HillasErrorBolt.spec(inputs=[hillas_bolt['errors']])
    # reco_bolt = RecoBolt.spec(inputs={hillas_bolt: Grouping.LOCAL_OR_SHUFFLE}, par=2)
