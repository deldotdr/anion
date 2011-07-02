"""
Pika Connection adapter for Twisted

This could be an adapter module in pika
"""

from twisted.internet import protocol

from pika.connection import Connection

class PikaAdapter(protocol.Protocol):

    def __init__(self, pika_connection):
        self.pika_connection = pika_connection
        #self.dataReceived = pika_connection._on_data_available

    def connectionMade(self):
        """
        """
        self.pika_connection._adapter = self
        self.pika_connection._on_connected()
        self.pika_connection.add_on_open_callback(self.connectionOpened)
        self.pika_connection.add_on_close_callback(self.connectionClosed)

    def connectionOpened(self, client):
        """
        This is an amqp open event. Might need to go somewhere else.
        """

    def connectionClosed(self):
        """
        Implement this to handle an amqp Connection.Close event.

        xxx Need an argument?
        """

    def connectionLost(self, reason):
        """
        """

    def dataReceived(self, data):
        """
        """
        self.pika_connection._on_data_available(data)

class TwistedConnection(Connection):
    """
    Is this the Protocol? Or the factory?
    Or something that wraps calling the reactor with a factory?
    """

    def _adapter_connect(self):
        """
        This is implemented to do nothing. We don't need this.
        """
        pass

    def _adapter__disconnect(self):
        """
        This is implemented to do nothing. We don't need this.
        """
        pass

    def _send_frame(self, frame):
        """
        This appends the fully generated frame to send to the broker to the
        output buffer which will be then sent via the connection adapter
        """
        marshalled_frame = frame.marshal()
        self.bytes_sent += len(marshalled_frame)
        self.frames_sent += 1

        self._adapter.transport.write(marshalled_frame)


