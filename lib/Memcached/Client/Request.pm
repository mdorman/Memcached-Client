package Memcached::Client::Request;
# ABSTRACT: Encapsulate Memcached::Client requests

use strict;
use warnings;
use AnyEvent qw{};
use Carp qw{croak};
use Memcached::Client::Log qw{DEBUG};

=head1 SYNOPSIS

Memcached::Client::Request and its subclasses are responsible for
managing the completion of a particular request to the memcached
cluster.

=method new

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

sub new {
    my $self = bless {}, shift;

    $self->{command} = shift;
    DEBUG "Command is %s", $self->{command};

    $self->{client} = shift;

    DEBUG "Checking for condvar/callback";
    if (ref $_[-1] eq 'AnyEvent::CondVar' or ref $_[-1] eq 'CODE') {
        DEBUG "Found condvar/callback";
        $self->{cb} = pop;
    } else {
        DEBUG "Making own condvar";
        $self->{cb} = AE::cv;
        $self->{wait} = 1;
    }

    DEBUG "Processing arguments: %s", \@_;
    if ($self->init (@_)) {
        DEBUG "Arguments valid, submitting";
        $self->submit;
    } else {
        DEBUG "Arguments invalid, completing";
        $self->complete;
    }

    $self;
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
    my $result;
    if (exists $self->{result}) {
        $result = $self->{result};
    } elsif (exists $self->{default}) {
        $result = $self->{default};
    }
    $self->{cb}->($result);
}

=method generate

Returns a reference to an anonymous subroutine that curries in the
generators arguments (usually just a particular command, but perhaps
more) to the creation of a new object, and then immediately calls
C<wait()>.

Used by Memcached::Client to create its methods.

=cut

sub generate {
    my ($class, @args) = @_;
    croak "You must give me some arguments to curry\n" unless (@args);
    sub { $class->new (@args, @_)->wait }
}

=method init

Called by C<new()> to process any arguments and set up additional bits
of the data structure appropriately.  The default C<init()> does
nothing, and in fact always returns 0, meaning it will never be
considered to have succeeded.

=cut

sub init {
    my ($self) = @_;
    DEBUG "arguments are %s", \@_;
    return 0;
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
    my ($self, $class, $rawkey, @args) = @_;
    my $key = (ref $rawkey ? $rawkey->[1] : $rawkey);
    DEBUG "Noting that we are waiting on %s", $key;
    $self->{partial}->{$key} = 1;
    my $new = "Memcached::Client::Request::$class";
    $new->new ($self->{command}, $self->{client}, $rawkey, @args, sub {
        my ($value) = @_;
        local *__ANON__ = "Memcached::Client::Request::multicall::callback";
        DEBUG "Noting that we received %s for %s", $value, $key;
        $self->{result}->{$key} = $value if (defined $value);
        delete $self->{partial}->{$key};
        $self->complete unless keys %{$self->{partial}};
    });
}

=method result

=cut

sub result {
    my ($self, $value) = @_;
    DEBUG "$self received result %s", $value;
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
    DEBUG "Submitting";
    $self->{client}->{servers}->{$self->{server}}->enqueue ($self);
}

=method wait

C<wait()> uses the object's AnyEvent::CondVar to drop into the event
loop to run the request if necessary (that is, there was no callback
or condvar when the request was constructed).  Otherwise, it simply
returns.

=cut

sub wait {
    my ($self) = @_;
    if ($self->{wait}) {
        DEBUG "Waiting for result";
        $self->{cb}->recv;
    }
}

package Memcached::Client::Request::AddMulti;
# ABSTRACT: Class to manage Memcached::Client server request

use Memcached::Client::Log qw{DEBUG};
use base qw{Memcached::Client::Request};

sub init {
    my ($self, $tuples) = @_;
    $self->{default} = {};
    DEBUG "Tuples are %s", $tuples;
    $self->{tuples} = $tuples;
    return scalar @{$tuples};
}

sub submit {
    my ($self) = @_;
    DEBUG "Submitting";
    for my $tuple (@{$self->{tuples}}) {
        $self->multicall (AddSingle => @{$tuple});
    }
}

package Memcached::Client::Request::AddSingle;
# ABSTRACT: Class to manage Memcached::Client server request

use Memcached::Client::Log qw{DEBUG};
use base qw{Memcached::Client::Request};

sub init {
    my ($self, $key, $value, $expiration) = @_;
    $self->{type} = "__add";
    $self->{default} = 0;
    $self->{rawkey} = $key;
    @{$self}{qw{key server}} = $self->{client}->__hash ($key);
    @{$self}{qw{command data flags}} = $self->{client}->{compressor}->compress ($self->{client}->{serializer}->serialize ($self->{command}, $value));
    $self->{expiration} = int ($expiration || 0);
    DEBUG "Argumentatives: %s", {map {$_, $self->{$_}} qw{rawkey key server command data flags expiration}};
    return ($self->{server} and $self->{key} and $self->{data});
}

package Memcached::Client::Request::BroadcastMulti;
# ABSTRACT: Class to manage Memcached::Client server request

use Memcached::Client::Log qw{DEBUG};
use base qw{Memcached::Client::Request};

sub init {
    my ($self, $command, @arguments) = @_;
    $self->{default} = {};
    $self->{arguments} = \@arguments;
    return scalar keys %{$self->{client}->{servers}};
}

sub submit {
    my ($self) = @_;
    for my $server (keys %{$self->{client}->{servers}}) {
        $self->multicall (BroadcastSingle => $server, @{$self->{arguments}});
    }
}

package Memcached::Client::Request::BroadcastSingle;
# ABSTRACT: Class to manage Memcached::Client server request

use Memcached::Client::Log qw{DEBUG};
use base qw{Memcached::Client::Request};

sub init {
    my ($self, $server, @arguments) = @_;
    DEBUG "Arguments are: %s", [$server, @arguments];
    $self->{type} = "__$self->{command}";
    $self->{default} = undef;
    $self->{server} = $server;
    $self->{arguments} = \@arguments;
    return exists $self->{client}->{servers}->{$self->{server}};
}

package Memcached::Client::Request::DecrMulti;
# ABSTRACT: Class to manage Memcached::Client server request

use Memcached::Client::Log qw{DEBUG};
use base qw{Memcached::Client::Request};

sub init {
    my ($self, $tuples) = @_;
    DEBUG "arguments are %s", $tuples;
    $self->{default} = {};
    $self->{tuples} = $tuples;
    return scalar @{$tuples};
}

sub submit {
    my ($self) = @_;
    for my $tuple (@{$self->{tuples}}) {
        $self->multicall (DecrSingle => @{$tuple});
    }
}

package Memcached::Client::Request::DecrSingle;
# ABSTRACT: Class to manage Memcached::Client server request

use Memcached::Client::Log qw{DEBUG};
use base qw{Memcached::Client::Request};

sub init {
    my ($self, $key, $delta, $initial) = @_;
    DEBUG "arguments are %s", \@_;
    $self->{type} = "__decr";
    $self->{default} = undef;
    $self->{rawkey} = $key;
    @{$self}{qw{key server}} = $self->{client}->__hash ($key);
    $self->{delta} = int ($delta || 1);
    $self->{data} = defined $initial ? int ($initial) : undef;
    return ($self->{server} and $self->{key} and $self->{delta});
}

package Memcached::Client::Request::DeleteMulti;
# ABSTRACT: Class to manage Memcached::Client server request

use Memcached::Client::Log qw{DEBUG};
use base qw{Memcached::Client::Request};

sub init {
    my ($self, @keys) = @_;
    DEBUG "arguments are %s", \@_;
    $self->{default} = {};
    $self->{keys} = \@keys;
    return scalar @keys;
}

sub submit {
    my ($self) = @_;
    for my $key (@{$self->{keys}}) {
        $self->multicall (DeleteSingle => $key);
    }
}

package Memcached::Client::Request::DeleteSingle;
# ABSTRACT: Class to manage Memcached::Client server request

use Memcached::Client::Log qw{DEBUG};
use base qw{Memcached::Client::Request};

sub init {
    my ($self, $key) = @_;
    DEBUG "arguments are %s", \@_;
    $self->{type} = "__delete";
    $self->{default} = 0;
    $self->{rawkey} = $key;
    @{$self}{qw{key server}} = $self->{client}->__hash ($key);
    return ($self->{server} and $self->{key});
}

package Memcached::Client::Request::GetMulti;
# ABSTRACT: Class to manage Memcached::Client server request

use Memcached::Client::Log qw{DEBUG};
use base qw{Memcached::Client::Request};

sub init {
    my ($self, @keys) = @_;
    DEBUG "arguments are %s", \@_;
    $self->{default} = {};
    $self->{keys} = \@keys;
    return scalar @keys;
}

sub submit {
    my ($self) = @_;
    for my $key (@{$self->{keys}}) {
        $self->multicall (GetSingle => $key);
    }
}

package Memcached::Client::Request::GetSingle;
# ABSTRACT: Class to manage Memcached::Client server request

use Memcached::Client::Log qw{DEBUG};
use base qw{Memcached::Client::Request};

sub init {
    my ($self, $key) = @_;
    DEBUG "arguments are %s", \@_;
    $self->{type} = "__get";
    $self->{default} = undef;
    $self->{rawkey} = $key;
    @{$self}{qw{key server}} = $self->{client}->__hash ($key);
    return ($self->{server} and $self->{key});
}

sub result {
    my ($self, $data, $flags, $cas) = @_;
    $self->{result} = $self->{client}->{serializer}->deserialize ($self->{client}->{compressor}->decompress ($data, $flags));
}

1;
