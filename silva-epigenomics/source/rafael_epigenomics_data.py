"""

"""

__author__ = 'Carl Witt'
__email__ = 'wittcarl@deneb.uberspace.de'

working_dirs = [
	("hep", "../data/hep-100000/cc/pegasus/genome-dax/20160831T122313+0000"),
	("hep", "../data/hep-100000/cc/pegasus/genome-dax/20160831T124807+0000"),
	("hep", "../data/hep-100000/cc/pegasus/genome-dax/20160831T134956+0000"),
	("hep", "../data/hep-100000/cc/pegasus/genome-dax/20160831T144949+0000"),
	("hep", "../data/hep-100000/cc/pegasus/genome-dax/20160831T155619+0000"),
	("ilmn","../data/ilmn-100000/cc/pegasus/genome-dax/20160831T162819+0000"),
	("ilmn","../data/ilmn-100000/cc/pegasus/genome-dax/20160831T165523+0000"),
	("ilmn","../data/ilmn-100000/cc/pegasus/genome-dax/20160831T172256+0000"),
	("ilmn","../data/ilmn-100000/cc/pegasus/genome-dax/20160831T175026+0000"),
	("ilmn","../data/ilmn-100000/cc/pegasus/genome-dax/20160831T185419+0000"),
	("taq", "../data/taq-100000/cc/pegasus/genome-dax/20160831T042527+0000"),
	("taq", "../data/taq-100000/cc/pegasus/genome-dax/20160831T044003+0000"),
	("taq", "../data/taq-100000/cc/pegasus/genome-dax/20160831T050311+0000"),
	("taq", "../data/taq-100000/cc/pegasus/genome-dax/20160831T051239+0000"),
	("taq", "../data/taq-100000/cc/pegasus/genome-dax/20160831T051246+0000"),
]
transformations = [
	#TODO some are missing, add them here
	"genome::filterContams:1.0",
	"genome::sol2sanger:1.0",
	"genome::fast2bfq:1.0",
	"genome::map:1.0",
 ]
