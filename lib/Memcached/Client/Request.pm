package Memcached::Client::Request;
# ABSTRACT: Driver for Memcached::Client requests

use strict;
use warnings;
use AnyEvent qw{};
use Carp qw{croak};
use Memcached::Client::Log qw{DEBUG};

=head1 SYNOPSIS

Memcached::Client::Request and its subclasses are responsible for
managing the completion of a given request to the memcached cluster.

=method generate

Returns a reference to an anonymous subroutine that creates a new
object in a Memcache::Client::Request subclass, currying in the
command that is specified as the argument to C<generate()>, and
expecting to be invoked as a method on a Memcache::Client object.

Each subclass of Memcache::Client::Request is responsible for
installing whatever commands it knows how to implement into the
Memcached::Client namespace.

Used by Memcached::Client to create its methods.

C<new()> builds a new request object.

It takes a memcache command name as its first parameter---this is the
sort of request it will be prepared to handle.  This is intended to be
curried into a subroutines that actually build objects.

It then expects a reference to the Memcached::Client object (since we
will need to call back into it).

It then examines the last argument in the argument list, and if it's a
code callback or an AnyEvent::CondVar, a reference to the callback or
condvar is stored for use on completion of the request.  Otherwise, an
AnyEvent::CondVar is created, and the request is marked as needing to
manage its own event looping.

The package's C<init()> routine is called with the remainder of the
arguments, then the C<submit()> routine is called to start the process
rolling, and then the object is returned.

For most uses, one can simply call ->wait on the result directly, and
C<generate()> creates anonymous functions that do just that.

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

=method complete

C<complete()> is by the connection code when the request is finished,
and it is responsible for invoking the callback to submit the results
to their consumer.

If there has been no result gathered, it will return the default if
there is one.

=cut

sub complete {
    my ($self) = @_;
    ##DEBUG "Request is completed";
    if (exists $self->{result}) {
        $self->{cb}->($self->{result})
    } elsif (exists $self->{default}) {
        $self->{cb}->($self->{default})
    } else {
        $self->{cb}->();
    }
}

=method multicall

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
    my $command = $self->{type};
    $self->{client}->{protocol}->$command ($self, $connection);
}

=method submit 

A default submit routine.  Adequate for the single request classes,
but it needs to be overridden for the aggregate requests

=cut

sub submit {
    my ($self) = @_;
    ##DEBUG "arguments are %s", \@_;
    return 0;
    ##DEBUG "Submitting";
    $self->{client}->{servers}->{$self->{server}}->enqueue ($self);
}

package Memcached::Client::Request::Add;
# ABSTRACT: Class to manage Memcached::Client server request

use Memcached::Client::Log qw{DEBUG};
use base qw{Memcached::Client::Request};

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

1;
