package Memcached::Client;
# ABSTRACT: All-singing, all-dancing Perl client for Memcached

use strict;
use warnings;
use AnyEvent qw{};
use AnyEvent::Handle qw{};
use Memcached::Client::Connection qw{};
use Memcached::Client::Log qw{DEBUG};
use Memcached::Client::Request qw{};
use Module::Load qw{load};

=head1 SYNOPSIS

  use Memcached::Client;
  my $client = Memcached::Client->new ({servers => ['127.0.0.1:11211']});

  # Synchronous interface
  my $value = $client->get ($key);

  # Asynchronous-ish interface (using your own condvar)
  use AnyEvent;
  my $cv = AnyEvent->cv;
  $client->get ($key, $cv);
  my $value = $cv->recv;

  # Asynchronous (AnyEvent) interface (using callback)
  use AnyEvent;
  $client->get ($key, sub {
    my ($value) = @_;
    warn "got $value for $key";
  });

  # You have to have an event loop running.
  my $loop = AnyEvent->cv;
  $loop->recv;

  # Done
  $client->disconnect();

=head1 DESCRIPTION

Memcached::Client attempts to be a versatile Perl client for the
memcached protocol.

It is built to be usable in a synchronous style by most Perl code,
while also being capable of being used as an entirely asynchronous
library running under AnyEvent.

In theory, being based on AnyEvent means that it can be integrated in
asynchrous programs running under EV, Event, POE, Glib, IO::Async,
etc., though it has only really been tested using AnyEvent's pure-Perl
and EV back-ends.

It allows for pluggable implementations of hashing, protcol,
serialization and compression---it currently implements the
traditional Cache::Memcached hashing, both text and binary protocols,
serialization using Storable or JSON, and compression using gzip.

=method new

C<new> takes a hash or a hashref containing any or all of the
following parameters, to define various aspects of the behavior of the
client.

=head3 parameters

=over 4

=item C<compress_threshold> => C<10_000>

Don't consider compressing items whose length is smaller than this
number.

=item C<compressor> => C<Gzip>

You may provide the name of the class to be instantiated by
L<Memcached::Client> to handle compressing data for the servers.

If the C<$classname> is prefixed by a C<+>, it will be used verbatim.
If it is not prefixed by a C<+>, we will look for the name under
L<Memcached::Client::Compressor>.

C<compressor> defaults to C<Gzip>, so a protocol object of the
L<Memcached::Client::Compressor::Gzip> type will be created by
default.  This is intended to be compatible with the behavior of
C<Cache::Memcached>.

=item C<namespace> => C<"">

This string will be used to prefix all keys stored or retrieved by
this client.

=item C<hash_namespace> => C<1>

If hash_namespace is true, any namespace prefix will be added to the
key B<before> hashing.  If it is false, any namespace prefix will be
added to the key B<after> hashing.

=item C<no_rehash> => C<1>

This parameter is only made available for compatiblity with
Cache::Memcached, and is ignored.  Memcached::Client will never
rehash.

=item C<preprocessor> => C<undef>

This allows you to set a preprocessor routine to normalize all keys
before they're sent to the server.  Expects a coderef that will
transform its first argument and then return it.  The identity
preprocessor would be:

 sub {
     return $_[0];
 }

This can be useful for mapping keys to a consistent case or encoding
them as to allow spaces in keys or the like.

=item C<procotol> => C<Text>

You may provide the name of the class to be instantiated by
L<Memcached::Client> to handle encoding details.

If the $classname is prefixed by a +, it will be used verbatim.  If it
is not prefixed by a +, we will look for the name under
L<Memcached::Client::Protocol>.

C<protocol> defaults to C<Text>, so a protocol object of the
L<Memcached::Client::Protocol::Text> type will be created by default.
This is intended to be compatible with the behavior of
C<Cache::Memcached>

=item C<readonly> => C<0>

This parameter is only made available for compatiblity with
Cache::Memcached, and is, for the moment, ignored.  Memcached::Client
does not currently have a readonly mode.

=item C<selector> => C<Traditional>

You may provide the name of the class to be instantiated by
L<Memcached::Client> to handle mapping keys to servers.

If the C<$classname> is prefixed by a C<+>, it will be used verbatim.
If it is not prefixed by a C<+>, we will look for the name under
L<Memcached::Client::Selector>.

C<selector> defaults to C<Traditional>, so a protocol object of the
L<Memcached::Client::Selector::Traditional> type will be created by
default.  This is intended to be compatible with the behavior of
C<Cache::Memcached>

=item C<serializer> => C<Storable>

You may provide the name of theclass to be instantiated by
L<Memcached::Client> to handle serializing data for the servers.

If the C<$classname> is prefixed by a C<+>, it will be used verbatim.
If it is not prefixed by a C<+>, we will look for the name under
L<Memcached::Client::Serializer>.

C<serializer> defaults to C<Storable>, so a protocol object of the
L<Memcached::Client::Serializer::Storable> type will be created by
default.  This is intended to be compatible with the behavior of
C<Cache::Memcached>.

=item C<servers> => \@servers

A reference to an array of servers to use.

Each item can either be a plain string in the form C<hostname:port>,
or an array reference of the form C<['hostname:port' =E<gt> weight]>.  In
the absence of a weight specification, it is assumed to be C<1>.

=back

=cut

sub new {
    my ($class, @args) = @_;
    my %args = 1 == scalar @args ? %{$args[0]} : @args;

    DEBUG "args: %s", \%args;

    my $self = bless {}, $class;

    # Get all of our objects instantiated
    $self->{compressor} = __class_loader (Compressor => $args{compressor} || 'Gzip')->new;
    $self->{serializer} = __class_loader (Serializer => $args{serializer} || 'Storable')->new;
    $self->{selector} = __class_loader (Selector => $args{selector} || 'Traditional')->new;
    $self->{protocol} = __class_loader (Protocol => $args{protocol} || 'Text')->new;

    $self->compress_threshold ($args{compress_threshold} || 10000);
    $self->hash_namespace ($args{hash_namespace} || 1);
    $self->namespace ($args{namespace} || "");
    $self->set_servers ($args{servers});
    $self->set_preprocessor ($args{preprocessor});

    DEBUG "Done creating object";

    $self;
}

# This manages class loading for the sub-classes
sub __class_loader {
    my ($prefix, $class) = @_;
    # Add our prefixes if the class name isn't called out as absolute
    $class = join ('::', 'Memcached::Client', $prefix, $class) if ($class !~ s/^\+//);
    # Sanitize our class name
    $class =~ s/[^\w:_]//g;
    DEBUG "Loading %s", $class;
    load $class;
    $class;
}

=method compress_threshold

This routine returns the current compress_threshold, and sets it to
the new value if it's handed one.

=cut

sub compress_threshold {
    my ($self, $new) = @_;
    DEBUG "Compress threshold %d", $new;
    $self->{compressor}->compress_threshold ($new);
}

=method namespace

This routine returns the current namespace, and sets it to the new
value if it's handed one.

=cut

sub namespace {
    my ($self, $new) = @_;
    my $ret = $self->{namespace};
    DEBUG "Namespace %s", $new;
    $self->{namespace} = $new if (defined $new);
    return $ret;
}

=method hash_namespace

Whether to prepend the namespace to the key before hashing, or after

This routine returns the current setting, and sets it to the new value
if it's handed one.

=cut

sub hash_namespace {
    my ($self, $new) = @_;
    my $ret = $self->{hash_namespace};
    DEBUG "Hash namespace %s", $new;
    $self->{hash_namespace} = !!$new if (defined $new);
    return $ret;
}

=method set_preprocessor

Sets a routine to preprocess keys before they are transmitted.

If you want to do some transformation to all keys before they hit the
wire, give this a subroutine reference and it will be run across all
keys.

=cut

sub set_preprocessor {
    my ($self, $new) = @_;
    $self->{preprocessor} = $new if (ref $new eq "CODE");
    return 1;
}

=method set_servers()

Change the list of servers to the listref handed to the function.

=cut

sub set_servers {
    my ($self, $servers) = @_;

    # Give the selector the list of servers first
    $self->{selector}->set_servers ($servers);

    # Shut down the servers that are no longer part of the list
    my $list = {map {(ref $_ ? $_->[0] : $_), {}} @{$servers}};
    for my $server (keys %{$self->{servers} || {}}) {
        next if (delete $list->{$server});
        DEBUG "Disconnecting %s", $server;
        my $connection = delete $self->{servers}->{$server};
        $connection->disconnect;
    }

    # Spawn connection handlers for all the others
    for my $server (sort keys %{$list}) {
        DEBUG "Connecting to %s", $server;
        $self->{servers}->{$server} ||= Memcached::Client::Connection->new ($server, $self->{protocol}->prepare_handle);
    }

    return 1;
}

=method connect()

Immediately initate connections to all servers.

While connections are implicitly made upon first need, and thus are
invisible to the user, it is sometimes helpful to go ahead and start
connections to all servers at once.  Calling C<connect()> will do
this.

=method disconnect()

Immediately disconnect from all handles and shutdown everything.

While connections are implicitly made upon first need, and thus are
invisible to the user, there are circumstances where it can be
important to call C<disconnect()> explicitly.

=cut

sub disconnect {
    my ($self) = @_;

    DEBUG "Disconnecting all servers";
    for my $server (keys %{$self->{servers}}) {
        next unless defined $self->{servers}->{$server};
        DEBUG "Disconnecting %s", $server;
        $self->{servers}->{$server}->disconnect;
    }
}

# When the object leaves scope, be sure to run C<disconnect()> to make
# certain that we shut everything down.
sub DESTROY {
    my $self = shift;
    $self->disconnect;
}

=head1 METHODS (INTERACTION)

All methods are intended to be called in either a synchronous or
asynchronous fashion.

A method is considered to have been called in a synchronous fashion if
it is does not have a callback (or AnyEvent::CondVar) as its last
parameter.  Because of the way the synchronous mode is implemented, it
B<must not> be used with programs that will call an event loop on
their own (often by calling C<-E<gt>recv> on a condvar)---you will
likely get an error:

	AnyEvent::CondVar: recursive blocking wait detected

If you call a method in a synchronous fashion, but from a void
context---that is, you are not doing anything with the return
value---a warning will be raised.

A method is considered to have been called in an asynchronous fashion
if it is called with a callback as its last parameter.  If you make a
call in asynchronous mode, your program is responsible for making sure
that an event loop is run...otherwise your program will simply hang.

If you call a method in an asynchronous fashion, but you are also
expecting a return value, a warning will be raised.

=method add

[$rc = ] add ($key, $value[, $exptime, $cb-E<gt>($rc) || $cv])

If the specified key does not already exist in the cache, it will be
set to the specified value.  If an expiration is included, it will
determine the lifetime of the object on the server.

If the add succeeds, 1 will be returned, if it fails, 0 will be
returned.

=method add_multi

[$rc = ] add_multi (\@([$key, $value, $exptime]), [$cb-E<gt>($rc) || $cv])

Given an arrayref of [key, value, $exptime] tuples, iterate over them
and if the specified key does not already exist in the cache, it will
be set to the specified value.  If an expiration is included, it will
determine the lifetime of the object on the server.

Returns a hashref of [key, boolean] tuples, where 1 means the add
succeeded, 0 means it failed.

=method append

[$rc = ] append ($key, $value[, $cb-E<gt>($rc) || $cv])

If the specified key already exists in the cache, it will have the
specified content appended to it.

If the append succeeds, 1 will be returned, if it fails, 0 will be
returned.

=method append_multi

[$rc = ] append_multi (\@([$key, $value, $exptime]), [$cb-E<gt>($rc) || $cv])

Given an arrayref of [key, value, $exptime] tuples, iterate over them
and if the specified key already exists in the cache, it will have the
the specified value appended to it.  If an expiration is included, it
will determine the lifetime of the object on the server.

Returns a hashref of [key, boolean] tuples, where 1 means the add
succeeded, 0 means it failed.

=method decr

[$value = ] decr ($key, [$delta (= 1), $initial, $cb-E<gt>($value) || $cv])

If the specified key already exists in the cache, it will be
decremented by the specified delta value, or 1 if no delta is
specified.

If the value does not exist in the cache, and an initial value is
supplied, the key will be set to that value.

If the decr succeeds, the resulting value will be returned, otherwise
undef will be the result.

=method decr_multi

[$value = ] decr_multi (\@($key, [$delta (= 1), $initial]), $cb-E<gt>($value) || $cv])

If the specified key already exists in the cache, it will be
decremented by the specified delta value, or 1 if no delta is
specified.

If the value does not exist in the cache, and an initial value is
supplied, the key will be set to that value.

If the decr succeeds, the resulting value will be returned, otherwise
undef will be the result.

=method delete

[$rc = ] delete ($key, [$cb-E<gt>($rc) || $cv])

If the specified key exists in the cache, it will be deleted.

If the delete succeeds, 1 will be returned, otherwise 0 will be the
result.

=method delete_multi

[\%keys = ] delete_multi (@keys, [$cb-E<gt>($rc) || $cv])

For each key specified, if the specified key exists in the cache, it
will be deleted.

If the delete succeeds, 1 will be returned, otherwise 0 will be the
result.

=method flush_all

[\%servers = ] flush_all ([$cb-E<gt>(\%servers) || $cv])

Clears the keys on each memcached server.

Returns a hashref indicating which servers the flush succeeded on.

=method get

[$value = ] get ($key, [$cb-E<gt>($value) || $cv])

Retrieves the specified key from the cache, otherwise returning undef.

=method get_multi

[\%values = ] get_multi (@values, [$cb-E<gt>(\%values) || $cv])

Retrieves the specified keys from the cache, returning a hashref of
key => value pairs.

=method incr

[$value = ] incr ($key, [$delta (= 1), $initial, $cb-E<gt>($value) || $cv])

If the specified key already exists in the cache, it will be
incremented by the specified delta value, or 1 if no delta is
specified.

If the value does not exist in the cache, and an initial value is
supplied, the key will be set to that value.

If the incr succeeds, the resulting value will be returned, otherwise
undef will be the result.

=method incr_multi

[$value = ] incr_multi (\@($key, [$delta (= 1), $initial]), $cb-E<gt>($value) || $cv])

If the specified key already exists in the cache, it will be
incremented by the specified delta value, or 1 if no delta is
specified.

If the value does not exist in the cache, and an initial value is
supplied, the key will be set to that value.

If the incr succeeds, the resulting value will be returned, otherwise
undef will be the result.

=method prepend($key, $value, $cb->($rc));

[$rc = ] append ($key, $value[, $cb-E<gt>($rc) || $cv])

If the specified key already exists in the cache, it will have the
specified content prepended to it.

If the prepend succeeds, 1 will be returned, if it fails, 0 will be
returned.

=method prepend_multi

[$rc = ] prepend_multi (\@([$key, $value, $exptime]), [$cb-E<gt>($rc) || $cv])

Given an arrayref of [key, value, $exptime] tuples, iterate over them
and if the specified key already exists in the cache, it will have the
the specified value prepended to it.  If an expiration is included, it
will determine the lifetime of the object on the server.

Returns a hashref of [key, boolean] tuples, where 1 means the add
succeeded, 0 means it failed.

=method remove

Alias to delete

=method replace

[$rc = ] replace ($key, $value[, $exptime, $cb-E<gt>($rc) || $cv])

If the specified key already exists in the cache, it will be replaced
by the specified value.  If it doesn't already exist, nothing will
happen.  If an expiration is included, it will determine the lifetime
of the object on the server.

If the replace succeeds, 1 will be returned, if it fails, 0 will be
returned.

=method replace_multi

[$rc = ] replace_multi (\@([$key, $value, $exptime]), [$cb-E<gt>($rc) || $cv])

Given an arrayref of [key, value, $exptime] tuples, iterate over them
and if the specified key already exists in the cache, it will be set
to the specified value.  If an expiration is included, it will
determine the lifetime of the object on the server.

Returns a hashref of [key, boolean] tuples, where 1 means the replace
succeeded, 0 means it failed.

=method set()

[$rc = ] set ($key, $value[, $exptime, $cb-E<gt>($rc) || $cv])

Set the specified key to the specified value.  If an expiration is
included, it will determine the lifetime of the object on the server.

If the set succeeds, 1 will be returned, if it fails, 0 will be
returned.

=method set_multi

[$rc = ] set_multi (\@([$key, $value, $exptime]), [$cb-E<gt>($rc) || $cv])

Given an arrayref of [key, value, $exptime] tuples, iterate over them
and set the specified key to the specified value.  If an expiration is
included, it will determine the lifetime of the object on the server.

Returns a hashref of [key, boolean] tuples, where 1 means the set
succeeded, 0 means it failed.

=method stats ()

[\%stats = ] stats ([$name, $cb-E<gt>(\%stats) || $cv])

Retrieves stats from all memcached servers.

Returns a hashref of hashrefs with the named stats.

=method version()

[\%versions = ] stats ([$cb-E<gt>(\%versions) || $cv])

Retrieves the version number from all memcached servers.

Returns a hashref of server => version pairs.

=cut

# We use this routine to select our server---it uses the selector to
# hash the key (assuming we are given a valid key, which it checks)
# and choose a machine.

sub __hash {
    my ($self, $key) = @_;
    $key = $self->{preprocessor}->($key) if ($self->{preprocessor});
    return unless (defined $key and # We must have some sort of key
                   (ref $key and # Pre-hashed
                    $key->[0] =~ m/^\d+$/ and # Hash is a decimal #
                    length $key->[1] > 0 and # Real key has a length
                    length $key->[1] <= 250 and # Real key is shorter than 250 chars
                    -1 == index $key, " " # Key contains no spaces
                   ) ||
                   (length $key > 0 and # Real key has a length
                    length $key <= 250 and # Real key is shorter than 250 chars
                    -1 == index $key, " " # Key contains no spaces
                   )
                  );
    return ($self->{namespace} . (ref $key ? $key->[1] : $key), $self->{selector}->get_server ($key, $self->{hash_namespace} ? $self->{namespace} : ""));
}

=head1 RATIONALE

Like the world needs another Memcached client for Perl.  Well, I hope
this one is worth inflicting on the world.

First there was L<Cache::Memcached>, the original implementation.

Then there was L<Cache::Memcached::Managed>, which was a layer on top
of L<Cache::Memcached> providing additional capablities.  Then people
tried to do it in XS, spawning L<Cache::Memcached::XS> and then
L<Cache::Memcached::Fast> and finally L<Memcached::libmemcached>,
based on the libmemcached C-library.  Then people tried to do it
asynchronously, spawning L<AnyEvent::Memcached> and
L<Cache::Memcached::AnyEvent>.  There are probably some I missed.

I have used all of them except for L<Cache::Memcached::Managed>
(because I didn't need its additional capabilities) and
L<Cache::Memcached::XS>, which never seems to have really gotten off
the ground, and L<Memcached::libmemcached> which went through long
periods of stagnation.  In fact, I've often worked with more than one
at a time, because my day job has both synchronous and asynchronous
memcached clients.

Diasuke Maki created the basics of a nice asynchronous implementation
of the memcached protocol as L<Cache::Memcached::AnyEvent>, and I
contributed some fixes to it, but it became clear to me that our
attitudes diverged on some things, and decided to fork the project
(for at its base I thought was some excellent code) to produce a
client that could support goals.

My intention with Memcached::Client is to create a reliable,
well-tested, well-documented, richly featured and fast Memcached
client library that can be used idiomatically in both synchronous and
asynchronous code, and should be configurabe to interoperate with
other clients.

I owe a great debt of gratitude to Diasuke Maki, as I used his
L<Cache::Memcached::AnyEvent> as the basis for this implementation,
though the code has basically been rewritten from the groune
up---which is to say, all bugs are mine.

=cut

1;
