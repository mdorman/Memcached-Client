package Memcached::Client::Connection;
# ABSTRACT: Class to manage Memcached::Client server connections

use strict;
use warnings;
use AnyEvent qw{};
use AnyEvent::Handle qw{};
use Memcached::Client::Log qw{DEBUG INFO};
use Scalar::Util qw{refaddr weaken};

=head1 SYNOPSIS

  use Memcached::Client::Connection;
  my $connection = Memcached::Client::Connection->new ("server:port");
  $connection->enqueue ($request->($handle, $callback), $failback);

=method new

C<new()> builds a new connection object.

Its only parameter is the server specification, in the form of
"hostname:port".  The object is constructed and returns immediately.

=cut

my $id = 0;

sub new {
    my ($class, $server, $preparation) = @_;
    my $self = bless {attempts => 0, id => $id++, last => 0, prepare => $preparation, requests => 0, queue => [], server => $server}, $class;
    ##DEBUG "Constructed";
    return $self;
}

=method connect

C<connect()> initiates a connection to the specified server.

If it succeeds, it will start dequeuing requests for the server to
satisfy.

If it fails, it will respond to all outstanding requests by invoking
their failback routine.

=cut

sub connect {
    my ($self, $callback) = @_;

    my ($host, $port) = split (/:/, $self->{server});
    $port ||= 11211;

    ##DEBUG "connecting to [%s]", $self->{server};

    $callback->() if $self->{handle};
    $self->{handle} ||= AnyEvent::Handle->new (connect => [$host, $port],
                                               keepalive => 1,
                                               on_connect => sub {
                                                   ##local *__ANON__ = "Memcached::Client::Connection::on_connect";
                                                   my ($handle, $host, $port) = @_;
                                                   ##DEBUG "connected";
                                                   $self->{attempts} = 0;
                                                   $self->{last} = 0;
                                                   $self->{requests} = 0;
                                                   $callback->() if ($callback);
                                                   $self->{executing}->run ($self) if ($self->{executing});
                                               },
                                               on_error => sub {
                                                   ##local *__ANON__ = "Memcached::Client::Connection::on_error";
                                                   my ($handle, $fatal, $message) = @_;
                                                   my $last = $self->{last} ? AE::time - $self->{last} : 0;
                                                   my $pending = scalar @{$self->{queue}};
                                                   # Need this here in case connection fails
                                                   if ($message eq "Broken pipe") {
                                                       ##DEBUG "broken pipe";
                                                       delete $self->{handle};
                                                       $self->connect;
                                                   } elsif ($message eq "Connection timed out" and ++$self->{attempts} < 5) {
                                                       ##DEBUG "reconnecting timeout";
                                                       delete $self->{handle};
                                                       $self->connect ($callback);
                                                   } else {
                                                       INFO "%d: %s, %d attempts, %d completed, %d pending, %f last", $self->{id}, $message, $self->{attempts}, $self->{requests}, $pending, $last;
                                                       $callback->() if ($callback);
                                                       delete $self->{handle};
                                                       $self->fail;
                                                   }
                                               },
                                               on_prepare => sub {
                                                   ##local *__ANON__ = "Memcached::Client::Connection::on_prepare";
                                                   my ($handle) = @_;
                                                   ##DEBUG "preparing handle";
                                                   $self->{prepare}->($handle) if ($self->{prepare});
                                                   return $self->{connect_timeout} || 0.5;
                                               });
}

=method disconnect

=cut

sub disconnect {
    my ($self) = @_;

    ##DEBUG "disconnecting";
    if (my $handle = $self->{handle}) {
        ##DEBUG "got handle";
        eval {
            $handle->stop_read;
            $handle->push_shutdown();
            $handle->destroy();
        };
    }

    ##DEBUG "failing all requests";
    $self->fail;
}

=method enqueue

C<enqueue()> adds the request specified (request, failback) pair to
the queue of requests to be processed.

=cut

sub enqueue {
    my ($self, $request) = @_;
    if ($self->{executing}) {
        ##DEBUG 'queueing request';
        $self->{last} = AE::time;
        push @{$self->{queue}}, $request;
    } else {
        $self->{executing} = $request;
        ##DEBUG "Executing";
        if ($self->{handle}) {
            $request->run ($self);
        } else {
            $self->connect;
        }
    }
}

=method complete

=cut

sub complete {
    my ($self) = @_;
    $self->{requests}++;
    ##DEBUG "Done with request";
    $self->{executing}->complete;
    delete $self->{executing};
    if ($self->{executing} = shift @{$self->{queue}}) {
        ##DEBUG "Executing";
        $self->{executing}->run ($self);
    }
}

=method fail

C<fail()> is called when there is an error on the handle, and it
invokes the failbacks of all queued requests.

=cut

sub fail {
    my ($self) = @_;
    ##DEBUG "Failing requests";
    for my $request (grep {defined} delete $self->{executing}, @{$self->{queue}}) {
        ##DEBUG "Failing request %s", $request;
        $request->complete;
        undef $request;
    }
}

1;
