"""
Event count topology
"""

from streamparse import Topology

from bolts.wordcount import HillasBolt, RecoBolt, HillasErrorBolt, RecoErrorBolt, PerfBolt
from spouts.words import EventSpout


class CTATopology(Topology):
    word_spout = EventSpout.spec()
    hillas_bolt = HillasBolt.spec(inputs=[word_spout])
    reco_bolt = RecoBolt.spec(inputs=[hillas_bolt])
    perf_bolt = PerfBolt.spec(inputs=[reco_bolt])
    # count events that have gone missing
    error_bolt = HillasErrorBolt.spec(inputs=[hillas_bolt['errors']])
    reco_error_bolt = RecoErrorBolt.spec(inputs=[reco_bolt['errors']])
