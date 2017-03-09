"""Example for using pydomintell."""
import domintell.domintell as domintell


def event(update_type, nid):
    """Callback for deth01 updates."""
    print(update_type + " \"" + str(nid) + "\" => " + 
          str(GATEWAY.sensors[nid]["value"]))

# To create a UDP gateway.
GATEWAY = domintell.Deth01Gateway(
    '192.168.0.247', event, True)

GATEWAY.debug = True
GATEWAY.start()
# To set sensor 1, child 1, sub-type V_LIGHT (= 2), with value 1.
# GATEWAY.set_child_value(1, 1, 2, 1)
# GATEWAY.stop()
