"""

"""

__author__ = 'Carl Witt'
__email__ = 'wittcarl@deneb.uberspace.de'

def on_session_destroyed(session_context):
    ''' If present, this function is called when a session is closed. '''
    print("session destroyed")