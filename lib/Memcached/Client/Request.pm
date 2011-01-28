package Memcached::Client::Request;
# ABSTRACT: Base class for Memcached::Client request drivers

use strict;
use warnings;
use AnyEvent qw{};
use Carp qw{croak};
use Memcached::Client::Log qw{DEBUG};

=head1 SYNOPSIS

Memcached::Client::Request and its subclasses are responsible for
managing the completion of a given request to the memcached cluster.

=method C<generate>

Returns a reference to an anonymous subroutine that creates a new
object in a C<Memcache::Client::Request> subclass, currying in the
command that is specified as the argument to C<generate>, and
expecting to be invoked as a method on a C<Memcache::Client> object.

Each subclass of C<Memcache::Client::Request> is responsible for using
C<generate> to install whatever commands it knows how to implement
into the C<Memcached::Client> namespace.

The resulting subroutine takes a C<Memcache::Client> object as its
first parameter, since we will need to call back into it.

It then examines the last argument in the argument list, and if it's a
C<coderef> or an C<AnyEvent::CondVar>, it is removed and stored for
use on completion of the request.  Otherwise, an C<AnyEvent::CondVar>
is created, and the request is marked as needing to manage its own
event looping.

The package's C<submit> routine is then called with the remainder of
the arguments.  If C<submit> returns false, then the submission is
assumed to have failed and the objects C<complete> routine is called
to return a result.

Finally, if the request is marked as needing to manage its own event
looping, it will wait on the C<AnyEvent::CondVar> that it created
earlier.

=cut

sub generate {
    my ($class, $command) = @_;

    ##DEBUG "Class is %s, Command is %s", $class, $command;

    return sub {
        my ($client, @args) = @_;
        local *__ANON__ = "Memcached::Client::Request::${class}::new";

        my $self = {command => $command, client => $client};

        ##DEBUG "Checking for condvar/callback";
        if (ref $args[-1] eq 'AnyEvent::CondVar' or ref $args[-1] eq 'CODE') {
            ##DEBUG "Found condvar/callback";
            $self->{cb} = pop @args;
        } else {
            ##DEBUG "Making own condvar";
            $self->{cb} = AE::cv;
            $self->{wait} = 1;
        }

        bless $self, $class;

        ##DEBUG "Processing arguments: %s", \@args;
        $self->submit (@args) || $self->complete;

        ##DEBUG "Checking whether to wait";
        $self->{cb}->recv if ($self->{wait});
    }
}

=method C<complete>

C<complete> is called when the request is finished---regardless of
whether it succeeded or failed---and it is responsible for invoking
the callback to submit the results to their consumer.

If there has been no result gathered, it will return the default if
there is one.

=cut

sub complete {
    my ($self) = @_;
    ##DEBUG "Request is completed";
    if (exists $self->{result}) {
        ##DEBUG "Returning result %s", $self->{result};
        $self->{cb}->($self->{result})
    } elsif (exists $self->{default}) {
        ##DEBUG "Returning default %s", $self->{default};
        $self->{cb}->($self->{default})
    } else {
        ##DEBUG "Returning undef";
        $self->{cb}->();
    }
}

=method C<multicall>

A helper routine for aggregate requests to manage the multiple
responses.

Creates a new object of the specified class, using the command and
client already specified for the aggregate request, as well as the key
and any arguments specified, and it constructs an anonymous subroutine
that will deliver the individual response and complete the aggregate
request when all individual responses have been returned.

=cut

sub multicall {
    my ($self, $rawkey, @args) = @_;
    my $key = ref $rawkey ? $rawkey->[1] : $rawkey;
    ##DEBUG "Noting that we are waiting on %s", $key;
    $self->{partial}->{$key} = 1;
    my $command = $self->{command};
    $self->{client}->$command ($rawkey, @args, sub {
                                   my ($value) = @_;
                                   local *__ANON__ = "Memcached::Client::Request::multicall::callback";
                                   ##DEBUG "Noting that we received %s for %s", $value, $key;
                                   $self->{result}->{$key} = $value if (defined $value);
                                   delete $self->{partial}->{$key};
                                   $self->complete unless keys %{$self->{partial}};
                               });
}

=method C<result>

Intended to be called by the protocol methods, C<result> records the
result value that it is given, if it is given one.

=cut

sub result {
    my ($self, $value) = @_;
    ##DEBUG "$self received result %s", $value;
    $self->{result} = $value if (defined $value);
}

=method C<run>

Intended to be called by the connection methods, C<run> takes a
reference to the appropriate connection, and calls the protocol driver
to start the actual request.

=cut

sub run {
    my ($self, $connection) = @_;
    my $command = $self->{type};
    $self->{client}->{protocol}->$command ($self, $connection);
}

=method C<submit>

C<submit> is a virtual method, to be implemented by subclasses in
order to do any argument processing and then enqueue the request on
the appropriate server.

=cut

sub submit {
    my ($self) = @_;
    die "You must implement submit";
}

package Memcached::Client::Request::Add;
# ABSTRACT: Driver for add-style Memcached::Client requests

use Memcached::Client::Log qw{DEBUG};
use base qw{Memcached::Client::Request};

=method C<submit>

C<submit> accepts a key, value and expiration.  It computes the server
to receive the request using the key, encodes the data using the
specified serializer and compressor, and makes sure that the expiration
is 0 if not otherwise specified.

Assuming that all of server, key and data are present, it enqueues its
request.

=cut

sub submit {
    my ($self, $key, $value, $expiration) = @_;
    $self->{type} = "__add";
    $self->{default} = 0;
    @{$self}{qw{key server}} = $self->{client}->__hash ($key);
    @{$self}{qw{command data flags}} = $self->{client}->{compressor}->compress ($self->{client}->{serializer}->serialize ($self->{command}, $value));
    $self->{expiration} = int ($expiration || 0);
    ##DEBUG "Argumentatives: %s", {map {$_, $self->{$_}} qw{key server command data flags expiration}};
    return unless ($self->{server} and $self->{key} and $self->{data});
    $self->{client}->{servers}->{$self->{server}}->enqueue ($self);
}

*Memcached::Client::add = Memcached::Client::Request::Add->generate ("add");
*Memcached::Client::append = Memcached::Client::Request::Add->generate ("append");
*Memcached::Client::prepend = Memcached::Client::Request::Add->generate ("prepend");
*Memcached::Client::replace = Memcached::Client::Request::Add->generate ("replace");
*Memcached::Client::set = Memcached::Client::Request::Add->generate ("set");

package Memcached::Client::Request::AddMulti;
# ABSTRACT: Class to manage Memcached::Client server request

use Memcached::Client::Log qw{DEBUG};
use base qw{Memcached::Client::Request};

sub submit {
    my ($self, $tuples) = @_;
    $self->{default} = {};
    ##DEBUG "Tuples are %s", $tuples;
    return unless scalar @{$tuples};
    for my $tuple (@{$tuples}) {
        $self->multicall (@{$tuple});
    }
    return 1;
}

*Memcached::Client::add_multi = Memcached::Client::Request::AddMulti->generate ("add");
*Memcached::Client::append_multi = Memcached::Client::Request::AddMulti->generate ("append");
*Memcached::Client::prepend_multi = Memcached::Client::Request::AddMulti->generate ("prepend");
*Memcached::Client::replace_multi = Memcached::Client::Request::AddMulti->generate ("replace");
*Memcached::Client::set_multi = Memcached::Client::Request::AddMulti->generate ("set");

package Memcached::Client::Request::Decr;
# ABSTRACT: Class to manage Memcached::Client server request

use Memcached::Client::Log qw{DEBUG};
use base qw{Memcached::Client::Request};

=method C<submit>

C<submit> accepts a key, delta and initial value.  It computes the
server to receive the request using the key, makes sure that the delta
is 1 if not otherwise specified, and make sure that the initial value
is an integer

Assuming that all of server, key and delta are present, it enqueues
its request.

=cut

sub submit {
    my ($self, $key, $delta, $initial) = @_;
    ##DEBUG "arguments are %s", \@_;
    $self->{type} = "__decr";
    $self->{default} = undef;
    @{$self}{qw{key server}} = $self->{client}->__hash ($key);
    $self->{delta} = int ($delta || 1);
    $self->{data} = defined $initial ? int ($initial) : undef;
    return unless ($self->{server} and $self->{key} and $self->{delta});
    $self->{client}->{servers}->{$self->{server}}->enqueue ($self);
}

*Memcached::Client::decr = Memcached::Client::Request::Decr->generate ("decr");
*Memcached::Client::incr = Memcached::Client::Request::Decr->generate ("incr");

package Memcached::Client::Request::DecrMulti;
# ABSTRACT: Class to manage Memcached::Client server request

use Memcached::Client::Log qw{DEBUG};
use base qw{Memcached::Client::Request};

sub submit {
    my ($self, $tuples) = @_;
    ##DEBUG "arguments are %s", $tuples;
    $self->{default} = {};
    return unless scalar @{$tuples};
    for my $tuple (@{$tuples}) {
        $self->multicall (@{$tuple});
    }
    return 1;
}

*Memcached::Client::decr_multi = Memcached::Client::Request::DecrMulti->generate ("decr");
*Memcached::Client::incr_multi = Memcached::Client::Request::DecrMulti->generate ("incr");

package Memcached::Client::Request::Delete;
# ABSTRACT: Class to manage Memcached::Client server request

use Memcached::Client::Log qw{DEBUG};
use base qw{Memcached::Client::Request};

=method C<submit>

C<submit> accepts a key.  It computes the server to receive the
request using the key.

Assuming that the server and key are present, it enqueues its request.

=cut

sub submit {
    my ($self, $key) = @_;
    ##DEBUG "arguments are %s", \@_;
    $self->{type} = "__delete";
    $self->{default} = 0;
    @{$self}{qw{key server}} = $self->{client}->__hash ($key);
    return unless ($self->{server} and $self->{key});
    $self->{client}->{servers}->{$self->{server}}->enqueue ($self);
}

*Memcached::Client::delete = Memcached::Client::Request::Delete->generate ("delete");

package Memcached::Client::Request::DeleteMulti;
# ABSTRACT: Class to manage Memcached::Client server request

use Memcached::Client::Log qw{DEBUG};
use base qw{Memcached::Client::Request};

sub submit {
    my ($self, @keys) = @_;
    ##DEBUG "arguments are %s", \@_;
    $self->{default} = {};
    return unless scalar @keys;
    for my $key (@keys) {
        $self->multicall ($key);
    }
    return 1;
}

*Memcached::Client::delete_multi = Memcached::Client::Request::DeleteMulti->generate ("delete");

package Memcached::Client::Request::Get;
# ABSTRACT: Class to manage Memcached::Client server request

use Memcached::Client::Log qw{DEBUG};
use base qw{Memcached::Client::Request};

=method C<submit>

C<submit> accepts a key.  It computes the server to receive the
request using the key.

Assuming that the server and key are present, it enqueues its request.

=cut

sub submit {
    my ($self, $key) = @_;
    ##DEBUG "arguments are %s", \@_;
    $self->{type} = "__get";
    $self->{default} = undef;
    @{$self}{qw{key server}} = $self->{client}->__hash ($key);
    return unless ($self->{server} and $self->{key});
    $self->{client}->{servers}->{$self->{server}}->enqueue ($self);
}

sub result {
    my ($self, $data, $flags, $cas) = @_;
    $self->{result} = $self->{client}->{serializer}->deserialize ($self->{client}->{compressor}->decompress ($data, $flags));
}

*Memcached::Client::get = Memcached::Client::Request::Get->generate ("get");

package Memcached::Client::Request::GetMulti;
# ABSTRACT: Class to manage Memcached::Client server request

use Memcached::Client::Log qw{DEBUG};
use base qw{Memcached::Client::Request};

sub submit {
    my ($self, @keys) = @_;
    ##DEBUG "arguments are %s", \@_;
    $self->{default} = {};
    return unless scalar @keys;
    for my $key (@keys) {
        $self->multicall ($key);
    }
    return 1;
}

*Memcached::Client::get_multi = Memcached::Client::Request::GetMulti->generate ("get");

package Memcached::Client::Request::Broadcast;
# ABSTRACT: Class to manage Memcached::Client server request

use Memcached::Client::Log qw{DEBUG};
use base qw{Memcached::Client::Request};

=method C<submit>

C<submit> accepts a command.  Assuming that there is a list of
servers, it iterates over all the presently known servers, enqueuing
the request

=cut

sub submit {
    my ($self, $command, @arguments) = @_;
    $self->{type} = "__$self->{command}";
    $self->{default} = {};
    $self->{arguments} = \@arguments;
    return unless keys %{$self->{client}->{servers}};
    for my $server (keys %{$self->{client}->{servers}}) {
        my $request = Memcached::Client::Request::BroadcastRequest->new ($self, $server);
        $self->{client}->{servers}->{$server}->enqueue ($request);
        $self->{partial}->{$server}++;
    }
    return 1;
}

=method result

Our specialized submit routine, that constructs a BroadcastRequest for
each server and enqueues it.

=cut

sub result {
    my ($self, $server, $value) = @_;
    ##DEBUG "Server %s gave result %s", $server, $value;
    $self->{result}->{$server} = $value if (defined $value);
    delete $self->{partial}->{$server};
    $self->complete unless keys %{$self->{partial}};
}

*Memcached::Client::flush_all = Memcached::Client::Request::Broadcast->generate ("flush_all");
*Memcached::Client::stats = Memcached::Client::Request::Broadcast->generate ("stats");
*Memcached::Client::version = Memcached::Client::Request::Broadcast->generate ("version");

package Memcached::Client::Request::BroadcastRequest;
# ABSTRACT: Class to manage Memcached::Client server request

use Memcached::Client::Log qw{DEBUG};

sub new {
    my ($class, $command, $server) = @_;
    my $self = {command => $command, server => $server};
    bless $self, $class;
}

=method complete

=cut

sub complete {
    my ($self) = @_;
    $self->{command}->result ($self->{server}, $self->{result});
}

=method result

=cut

sub result {
    my ($self, $value) = @_;
    ##DEBUG "$self received result %s", $value;
    $self->{result} = $value if (defined $value);
}

=method run

=cut

sub run {
    my ($self, $connection) = @_;
    my $command = $self->{command}->{type};
    $self->{command}->{client}->{protocol}->$command ($self, $connection);
}

package Memcached::Client::Request::Connect;
# ABSTRACT: Class to manage Memcached::Client connection request

use Memcached::Client::Log qw{DEBUG};
use base qw{Memcached::Client::Request};

=method C<submit>

C<submit> accepts a command.  Assuming that there is a list of
servers, it iterates over all the presently known servers, enqueuing
the request

=cut

sub submit {
    my ($self) = @_;
    $self->{default} = 1;
    return unless keys %{$self->{client}->{servers}};
    for my $server (keys %{$self->{client}->{servers}}) {
        $self->{partial}->{$server}++;
        $self->{client}->{servers}->{$server}->connect (sub {
                                                            local *__ANON__ = "Memcached::Client::connect::callback";
                                                            DEBUG "%s connected", $server;
                                                            delete $self->{partial}->{$server};
                                                            $self->complete unless keys %{$self->{partial}};
                                                        });
    }
    return 1;
}

*Memcached::Client::connect = Memcached::Client::Request::Connect->generate ("connect");

1;
