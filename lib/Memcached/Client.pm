package Memcached::Client;
BEGIN {
  $Memcached::Client::VERSION = '1.05';
}
# ABSTRACT: All-singing, all-dancing Perl client for Memcached

use strict;
use warnings;
use AnyEvent qw{};
use AnyEvent::Handle qw{};
use Carp qw{carp cluck};
use Memcached::Client::Connection qw{};
use Memcached::Client::Log qw{DEBUG};
use Module::Load;


sub new {
    my ($class, @args) = @_;
    my %args = 1 == scalar @args ? %{$args[0]} : @args;

    DEBUG "C: new - %s", \%args;

    cluck "You declared a callback but are also expecting a return value" if ($args{callback} and defined wantarray);

    my $self = bless {}, $class;

    # Get all of our objects instantiated
    $self->{serializer} = __class_loader (Serializer => $args{serializer} || 'Storable')->new;
    $self->{selector} = __class_loader (Selector => $args{selector} || 'Traditional')->new;
    $self->{protocol} = __class_loader (Protocol => $args{protocol} || 'Text')->new;

    $self->compress_threshold ($args{compress_threshold} || 10000);
    $self->hash_namespace ($args{hash_namespace} || 1);
    $self->namespace ($args{namespace} || "");
    $self->set_servers ($args{servers});
    $self->set_preprocessor ($args{preprocessor});

    DEBUG "C: Done creating object";

    $self;
}

# This manages class loading for the sub-classes
sub __class_loader {
    my ($prefix, $class) = @_;
    # Add our prefixes if the class name isn't called out as absolute
    $class = join ('::', 'Memcached::Client', $prefix, $class) if ($class !~ s/^\+//);
    # Sanitize our class name
    $class =~ s/[^\w:_]//g;
    load $class;
    $class;
}


sub compress_threshold {
    my ($self, $new) = @_;
    $self->{serializer}->compress_threshold ($new);
}


sub namespace {
    my ($self, $new) = @_;
    my $ret = $self->{namespace};
    $self->{namespace} = $new if (defined $new);
    return $ret;
}


sub hash_namespace {
    my ($self, $new) = @_;
    my $ret = $self->{hash_namespace};
    $self->{hash_namespace} = !!$new if (defined $new);
    return $ret;
}


sub set_preprocessor {
    my ($self, $new) = @_;
    $self->{preprocessor} = $new if (ref $new eq "CODE");
    return 1;
}


sub __preprocess {
    my ($self, $key) = @_;
    return $key unless $self->{preprocessor};
    return $self->{preprocessor}->($key);
}


sub set_servers {
    my ($self, $servers) = @_;

    $self->{selector}->set_servers ($servers);

    # Shut down the servers that are no longer part of the list
    my $list = {map {(ref $_ ? $_->[0] : $_), {}} @{$servers}};
    for my $server (keys %{$self->{servers} || {}}) {
        next if $list->{$server};
        my $connection = delete $self->{servers}->{$server};
        $connection->fail;
    }

    # Spawn connection handlers for all the others
    for my $server (keys %{$list}) {
        DEBUG "Creating connection for %s", $server;
        $self->{servers}->{$server} ||= Memcached::Client::Connection->new ($server, $self->{protocol}->prepare_handle);
    }

    return 1;
}


sub connect {
    my ($self, @args) = @_;

    DEBUG "C [connect]: Starting connection";

    my ($callback, $cmd_cv);
    if (ref $args[$#args] eq 'AnyEvent::CondVar') {
        $cmd_cv = pop @args;
        DEBUG "C [connect]: Found condvar";
        cluck "You gave us a condvar but are also expecting a return value" if (defined wantarray);
    } elsif (ref $args[$#args] eq 'CODE') {
        $callback = pop @args;
        DEBUG "C [connect]: Found callback";
        cluck "You declared a callback but are also expecting a return value" if (defined wantarray);
    }

    $cmd_cv ||= AE::cv;
    $cmd_cv->cb (sub {$callback->($cmd_cv->recv)}) if ($callback);

    $cmd_cv->begin (sub {$_[0]->send (1)});
    for my $server (keys %{$self->{servers}}) {
        DEBUG "C [connect]: Connecting %s", $server;
        $cmd_cv->begin;
        $self->{servers}->{$server}->connect (sub {
                                                  DEBUG "C [connect]: Done connecting %s", $server;
                                                  $cmd_cv->end
                                              });
    }
    $cmd_cv->end;

    DEBUG "C: %s", $callback ? "using callback" : "using condvar";
    $cmd_cv->recv unless ($callback or ($cmd_cv eq $_[$#_]));
}


sub disconnect {
    my ($self) = @_;

    for my $handle (map {delete $self->{servers}->{$_}->{handle}} keys %{$self->{servers}}) {
        next unless defined $handle;
        eval {
            $handle->stop_read;
            $handle->push_shutdown();
            $handle->destroy();
        };
    }
}

# When the object leaves scope, be sure to run C<disconnect()> to make
# certain that we shut everything down.
sub DESTROY {
    my $self = shift;
    $self->disconnect;
}

{
    # This sub generates the routines that correspond to our broadcast
    # methods---that is, those that are automatically sent to every
    # server.  It takes the name of the command as its only parameter.
    #
    # It first creates an AE::cv to represent command completion.  If
    # the command has been given a callback, and thus can assume we're
    # in asynchronous mode, the command CV will be a proxy for that
    # callback, otherwise we assume we're in synchronous mode, and a
    # ->recv on the command CV will be used to drive execution of the
    # request.
    #
    # For each server we know about, we increment the outstanding
    # requests in the command CV, and enqueue a request with
    # connection CV that has a callback that calls the connection's
    # completion callback, stores the results, and decrements the
    # outstanding requests.  The failback for the queue decrements the
    # outstanding requests.

    my $broadcast = sub {
        my ($command, $nowait) = @_;
        my $subname = "__$command";
        sub {
            local *__ANON__ = "Memcached::Client::$command";
            my ($self, @args) = @_;

            my ($callback, $cmd_cv);
            if (ref $args[$#args] eq 'AnyEvent::CondVar') {
                $cmd_cv = pop @args;
                DEBUG "C [%s]: Found condvar", $command;
                cluck "You gave us a condvar but are also expecting a return value" if (defined wantarray);
            } elsif (ref $args[$#args] eq 'CODE') {
                $callback = pop @args;
                DEBUG "C [%s]: Found callback", $command;
                cluck "You declared a callback but are also expecting a return value" if (defined wantarray);
            } elsif (!defined wantarray and !$nowait) {
                DEBUG "C [%s]: No callback or condvar: %s", $command, ref $args[$#args];
                cluck "You have no callback, but aren't waiting for a return value";
            }

            $cmd_cv ||= AE::cv;
            $cmd_cv->cb (sub {$callback->($cmd_cv->recv)}) if ($callback);

            my (%rv);

            $cmd_cv->begin (sub {$_[0]->send (\%rv)});
            for my $connection (values %{$self->{servers}}) {
                DEBUG "C [%s]: enqueuing to %s", $command, $connection->{server};
                $cmd_cv->begin;
                $connection->enqueue (sub {
                                          my ($handle, $completion, $server) = @_;
                                          my $connection_cv = AE::cv {
                                              $completion->();
                                              $rv{$server} = $_[0]->recv;
                                              $cmd_cv->end;
                                          };
                                          $self->{protocol}->$subname ($handle, $connection_cv, @args);
                                      }, sub {$cmd_cv->end});
            }
            $cmd_cv->end;
            DEBUG "C: %s", $callback ? "using callback" : "using condvar";
            $cmd_cv->recv unless ($callback or ($cmd_cv eq $_[$#_]));
        }
    };

    # This sub generates the routines that handle single entries.  It
    # takes the name of the command and a default value should the
    # request fail for some reason.
    #
    # It first creates an AE::cv to represent command completion.  If
    # the command has been given a callback, and thus can assume we're
    # in asynchronous mode, the command CV will be a proxy for that
    # callback, otherwise we assume we're in synchronous mode, and a
    # ->recv on the command CV will be used to drive execution of the
    # request.
    #
    # If the key we've been given is hashable to a connection, we call
    # the corresponding routine with a reference to our connection,
    # $key and any additional arguments.  If this doesn't return a
    # true value (further argument checks having failed, likely), we
    # send the default response through the command CV.
    #
    # If the key is not hashable, we send back our default response
    # through the command CV.

    my $keyed = sub {
        my ($command, $default, $nowait) = @_;
        my $subname = "__$command";
        sub {
            local *__ANON__ = "Memcached::Client::$command";
            my ($self, @args) = @_;

            my ($callback, $cmd_cv);
            if (ref $args[$#args] eq 'AnyEvent::CondVar') {
                $cmd_cv = pop @args;
                DEBUG "C [%s]: Found condvar", $command;
                cluck "You gave us a condvar but are also expecting a return value" if (defined wantarray);
            } elsif (ref $args[$#args] eq 'CODE') {
                $callback = pop @args;
                DEBUG "C [%s]: Found callback", $command;
                cluck "You declared a callback but are also expecting a return value" if (defined wantarray);
            } elsif (!defined wantarray and !$nowait) {
                DEBUG "C [%s]: No callback or condvar: %s", $command, ref $args[$#args];
                cluck "You have no callback, but aren't waiting for a return value";
            }

            # Even if we're given a callback, we proxy it through a CV of our own creation
            $cmd_cv ||= AE::cv;
            $cmd_cv->cb (sub {$callback->($cmd_cv->recv || $default)}) if ($callback);

            if (my ($key, $server) = $self->__hash (shift @args)) {
                DEBUG "C [%s]: %s", $command, join " ", map {defined $_ ? "[$_]" : "[undef]"} $key, @args;
                $self->$subname ($cmd_cv, wantarray, $self->{servers}->{$server}, $key, @args) or $cmd_cv->send ($default);
            } else {
                $cmd_cv->send ($default);
            }

            DEBUG "C [%s]: %s", $command, $callback ? "using callback" : "using condvar";
            ($cmd_cv->recv || $default) unless ($callback or ($cmd_cv eq $_[$#_]));
        }
    };

    # This sub generates the routines that handle multiple entries.
    # That will need to be hashed individually.  It takes the name of
    # the command as a parameter.
    #
    # It first creates an AE::cv to represent command completion.  If
    # the command has been given a callback, and thus can assume we're
    # in asynchronous mode, the command CV will be a proxy for that
    # callback, otherwise we assume we're in synchronous mode, and a
    # ->recv on the command CV will be used to drive execution of the
    # request.
    #
    # We then the corresponding routine with a reference to our
    # command CV all additional arguments.

    my $multi = sub {
        my ($command, $nowait) = @_;
        my $subname = "__${command}_multi";
        sub {
            local *__ANON__ = "Memcached::Client::$command";
            my ($self, @args) = @_;

            my ($callback, $cmd_cv);
            if (ref $args[$#args] eq 'AnyEvent::CondVar') {
                $cmd_cv = pop @args;
                DEBUG "C [%s]: Found condvar", $command;
                cluck "You gave us a condvar but are also expecting a return value" if (defined wantarray);
            } elsif (ref $args[$#args] eq 'CODE') {
                $callback = pop @args;
                DEBUG "C [%s]: Found callback", $command;
                cluck "You declared a callback but are also expecting a return value" if (defined wantarray);
            } elsif (!defined wantarray and !$nowait) {
                DEBUG "C [%s]: No callback or condvar: %s", $command, ref $args[$#args];
                cluck "You have no callback, but aren't waiting for a return value" ;
            }

            # Even if we're given a callback, we proxy it through a CV of our own creation
            $cmd_cv ||= AE::cv;
            $cmd_cv->cb (sub {$callback->($cmd_cv->recv)}) if ($callback);

            DEBUG "C: calling %s - %s", $subname, \@args;
            $self->$subname ($cmd_cv, wantarray, @args);

            DEBUG "C: %s", $callback ? "using callback" : "using condvar";
            $cmd_cv->recv unless ($callback or ($cmd_cv eq $_[$#_]));
        }
    };


    *add = $keyed->("add", 0, 1);


    *add_multi = $multi->("add", 1);


    *append = $keyed->("append", 0, 1);


    *append_multi = $multi->("append", 1);


    *decr = $keyed->("decr", undef, 0);


    *decr_multi = $multi->("decr", 0);


    *delete = $keyed->("delete", 0, 1);


    *delete_multi = $multi->("delete", 1);


    *flush_all = $broadcast->("flush_all", 1);


    *get = $keyed->("get", undef, 0);


    *get_multi = $multi->("get", 0);


    *incr = $keyed->("incr", undef, 0);


    *incr_multi = $multi->("incr", 0);


    *prepend = $keyed->("prepend", 0, 1);


    *prepend_multi = $multi->("prepend", 1);


    *remove = $keyed->("delete", 0, 1);


    *replace = $keyed->("replace", 0, 1);


    *replace_multi = $multi->("replace", 1);


    *set = $keyed->("set", 0, 1);


    *set_multi = $multi->("set", 1);


    *stats = $broadcast->("stats", 0);


    *version = $broadcast->("version", 0);
}

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
    return ($key, $self->{selector}->get_server ($key, $self->{hash_namespace} ? $self->{namespace} : ""));
}

{
    my $generator = sub {
        my ($command) = (@_);
        my $subname = "__$command";
        return sub {
            local *__ANON__ = "Memcached::Client::$subname";
            my ($self, $cmd_cv, $wantarray, $connection, $key, $value, $expiration) = @_;
            DEBUG "C [%s]: %s", $subname, join " ", map {defined $_ ? "[$_]" : "[undef]"} @_;
            $expiration = int ($expiration || 0);
            return unless (defined $value);
            $connection->enqueue (sub {
                                      my ($handle, $completion, $server) = @_;
                                      my ($data, $flags) = $self->{serializer}->serialize ($value, $command);
                                      my $server_cv = AE::cv {
                                          $completion->();
                                          $cmd_cv->send ($_[0]->recv);
                                      };
                                      $self->{protocol}->$subname ($handle, $server_cv, $self->{namespace} . (ref $key ? $key->[1] : $key), $data, $flags, $expiration);
                                  }, $cmd_cv);
        }
    };

    *__add     = $generator->("add");
    *__append  = $generator->("append");
    *__prepend = $generator->("prepend");
    *__replace = $generator->("replace");
    *__set     = $generator->("set");
}

{
    my $generator = sub {
        my ($command) = (@_);
        my $subname = "__$command";
        return sub {
            local *__ANON__ = "Memcached::Client::$subname";
            my ($self, $cmd_cv, $wantarray, $tuples) = @_;
            DEBUG "C [%s]: %s", $subname, join " ", map {defined $_ ? "[$_]" : "[undef]"} @_;
            my (%rv);
            $cmd_cv->begin (sub {$_[0]->send (\%rv)});
            DEBUG "Tuples are %s", $tuples;
            for my $tuple (@{$tuples}) {
                DEBUG "Tuple is %s", $tuple;
                if (my ($key, $server) = $self->__hash (shift @{$tuple})) {
                    my ($value, $expiration) = @{$tuple};
                    $expiration = int ($expiration || 0);
                    $rv{$key} = 0;
                    DEBUG "C: $command %s", $server;
                    $cmd_cv->begin;
                    $self->{servers}->{$server}->enqueue (sub {
                                                              my ($handle, $completion, $server) = @_;
                                                              my ($data, $flags) = $self->{serializer}->serialize ($value, $command);
                                                              my $server_cv = AE::cv {
                                                                  $completion->();
                                                                  $rv{$key} = $_[0]->recv;
                                                                  $cmd_cv->end;
                                                              };
                                                              $self->{protocol}->$subname ($handle, $server_cv, $self->{namespace} . (ref $key ? $key->[1] : $key), $data, $flags, $expiration);
                                                          }, sub {$cmd_cv->end});
                }
            }
            $cmd_cv->end;
        }
    };

    *__add_multi     = $generator->("add");
    *__append_multi  = $generator->("append");
    *__prepend_multi = $generator->("prepend");
    *__replace_multi = $generator->("replace");
    *__set_multi     = $generator->("set");
}

{
    my $generator = sub {
        my ($command) = (@_);
        my $subname = "__$command";
        return sub {
            local *__ANON__ = "Memcached::Client::$subname";
            my ($self, $cmd_cv, $wantarray, $connection, $key, $delta, $initial) = @_;
            DEBUG "C [%s]: %s", $subname, join " ", map {defined $_ ? "[$_]" : "[undef]"} @_;
            $delta = 1 unless defined $delta;
            $connection->enqueue (sub {
                                      my ($handle, $completion, $server) = @_;
                                      my $server_cv = AE::cv {
                                          $completion->();
                                          $cmd_cv->send ($_[0]->recv);
                                      };
                                      $self->{protocol}->$subname ($handle, $server_cv, $self->{namespace} . (ref $key ? $key->[1] : $key), $delta, $initial);
                                  }, $cmd_cv);
        }
    };

    *__decr = $generator->("decr");
    *__incr = $generator->("incr");
}

{
    my $generator = sub {
        my ($command) = (@_);
        my $subname = "__$command";
        return sub {
            local *__ANON__ = "Memcached::Client::$subname";
            my ($self, $cmd_cv, $wantarray, $tuples) = @_;
            DEBUG "C [%s]: %s", $subname, join " ", map {defined $_ ? "[$_]" : "[undef]"} @_;
            my (%rv);
            DEBUG "Begin on command CV to establish callback";
            $cmd_cv->begin (sub {$_[0]->send (\%rv)});
            DEBUG "Tuples are %s", $tuples;
            for my $tuple (@{$tuples}) {
                DEBUG "Tuple is %s", $tuple;
                if (my ($key, $server) = $self->__hash (shift @{$tuple})) {
                    DEBUG "keys is %s, server is %s", $key, $server;
                    my ($delta, $initial) = @{$tuple};
                    $delta = 1 unless defined $delta;
                    DEBUG "C: $command %s", $server;
                    DEBUG "Begin on command CV before enqueue";
                    $cmd_cv->begin;
                    $self->{servers}->{$server}->enqueue (sub {
                                                              my ($handle, $completion, $server) = @_;
                                                              my $server_cv = AE::cv {
                                                                  $completion->();
                                                                  $rv{$key} = $_[0]->recv;
                                                                  DEBUG "End on command CV from server CV";
                                                                  $cmd_cv->end;
                                                              };
                                                              $self->{protocol}->$subname ($handle, $server_cv, $self->{namespace} . (ref $key ? $key->[1] : $key), $delta, $initial);
                                                          }, sub {
                                                              DEBUG "End on command CV from error callback";
                                                              $cmd_cv->end
                                                          });
                }
            }

            DEBUG "End on command CV ";
            $cmd_cv->end;
        }
    };

    *__incr_multi = $generator->("incr");
    *__decr_multi = $generator->("decr");
}

sub __delete {
    my ($self, $cmd_cv, $wantarray, $connection, $key) = @_;
    DEBUG "C [delete]: %s", join " ", map {defined $_ ? "[$_]" : "[undef]"} @_;
    $connection->enqueue (sub {
                              my ($handle, $completion, $server) = @_;
                              my $server_cv = AE::cv {
                                  $completion->();
                                  $cmd_cv->send ($_[0]->recv);
                              };
                              $self->{protocol}->__delete ($handle, $server_cv, $self->{namespace} . (ref $key ? $key->[1] : $key));
                          }, $cmd_cv);
}

sub __delete_multi {
    my ($self, $cmd_cv, $wantarray, @keys) = @_;
    DEBUG "C [delete_multi]: %s", join " ", map {defined $_ ? "[$_]" : "[undef]"} @_;
    my (%rv);
    $cmd_cv->begin (sub {$_[0]->send (\%rv)});
    DEBUG "Keys are %s", \@keys;
    for my $key (@keys) {
        if (my ($key, $server) = $self->__hash ($key)) {
            DEBUG "key is %s", $key;
            $rv{$key} = 0;
            DEBUG "C: delete_multi %s", $server;
            $cmd_cv->begin;
            $self->{servers}->{$server}->enqueue (sub {
                                                      my ($handle, $completion, $server) = @_;
                                                      my $server_cv = AE::cv {
                                                          $completion->();
                                                          $rv{$key} = $_[0]->recv;
                                                          $cmd_cv->end;
                                                      };
                                                      $self->{protocol}->__delete ($handle, $server_cv, $self->{namespace} . (ref $key ? $key->[1] : $key));
                                                  }, sub {$cmd_cv->end});
        }
    }
    $cmd_cv->end;
}

sub __get {
    my ($self, $cmd_cv, $wantarray, $connection, $key) = @_;
    DEBUG "C [get]: %s", join " ", map {defined $_ ? "[$_]" : "[undef]"} @_;
    $connection->enqueue (sub {
                              my ($handle, $completion, $server) = @_;
                              my $server_cv = AE::cv {
                                  $completion->();
                                  if (my $result = $_[0]->recv) {
                                      DEBUG "C: get - result %s", $result;
                                      my $gotten = $result->{$self->{namespace} . (ref $key ? $key->[1] : $key)};
                                      $cmd_cv->send ($self->{serializer}->deserialize (@{$gotten}{qw{data flags}}));
                                  } else {
                                      $cmd_cv->send;
                                  }
                              };
                              $self->{protocol}->__get ($handle, $server_cv, $self->{namespace} . (ref $key ? $key->[1] : $key));
                          }, $cmd_cv);
}

sub __get_multi {
    my ($self, $cmd_cv, $wantarray, $keys) = @_;
    DEBUG "C [get_multi]: %s", join " ", map {defined $_ ? "[$_]" : "[undef]"} @_;
    my (%requests);
    for my $key (@{$keys}) {
        if (my ($key, $server) = $self->__hash ($key)) {
            push @{$requests{$server}}, $self->{namespace} . (ref $key ? $key->[1] : $key);
        }
    }

    my (%rv);
    $cmd_cv->begin (sub {$_[0]->send (\%rv)});
    for my $server (keys %requests) {
        DEBUG "C: get %s", $server;
        $cmd_cv->begin;
        $self->{servers}->{$server}->enqueue (sub {
                                                  my ($handle, $completion, $server) = @_;
                                                  my $server_cv = AE::cv {
                                                      $completion->();
                                                      if (my $result = $_[0]->recv) {
                                                          DEBUG "C: get - result %s", $result;
                                                          for my $key (keys %{$result}) {
                                                              next unless (defined $key and length $key);
                                                              my $stripped = substr $key, length $self->{namespace};
                                                              my $deserialized = $self->{serializer}->deserialize (@{$result->{$key}}{qw{data flags}});
                                                              next unless defined $deserialized;
                                                              $rv{$stripped} = $deserialized;
                                                          }
                                                      }
                                                      $cmd_cv->end;
                                                  };
                                                  $self->{protocol}->__get ($handle, $server_cv, @{$requests{$server}});
                                              }, sub {$cmd_cv->end});
    }
    $cmd_cv->end;
}


1;

__END__
=pod

=head1 NAME

Memcached::Client - All-singing, all-dancing Perl client for Memcached

=head1 VERSION

version 1.05

=head1 SYNOPSIS

  use Memcached::Client;
  my $client = Memcached::Client->new ({servers => ['127.0.0.1:11211']});

  # Synchronous interface
  my $value = $client->get ($key);

  # Asynchronous (AnyEvent) interface (using condvar)
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
and serialization using Storable, and compression using gzip.

=head1 METHODS

=head2 new

C<new> takes a hash or a hashref containing any or all of the
following parameters, to define various aspects of the behavior of the
client.

=head3 parameters

=over 4

=item C<compress_threshold> => C<10_000>

Don't consider compressing items whose length is smaller than this
number.

=item C<namespace> => C<"">

If namespace is set, it will be used to prefix all keys before
hashing.  This is not defined by default.

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

=head2 compress_threshold

This routine returns the current compress_threshold, and sets it to
the new value if it's handed one.

=head2 namespace

This routine returns the current namespace, and sets it to the new
value if it's handed one.

=head2 hash_namespace

Whether to prepend the namespace to the key before hashing, or after

This routine returns the current setting, and sets it to the new value
if it's handed one.

=head2 set_preprocessor

Sets a routine to preprocess keys before they are transmitted.

If you want to do some transformation to all keys before they hit the
wire, give this a subroutine reference and it will be run across all
keys.

=head2 __preprocess

Preprocess keys before they are transmitted.

=head2 set_servers()

Change the list of servers to the listref handed to the function.

=head2 connect()

Immediately initate connections to all servers.

While connections are implicitly made upon first need, and thus are
invisible to the user, it is sometimes helpful to go ahead and start
connections to all servers at once.  Calling C<connect()> will do
this.

=head2 disconnect()

Immediately disconnect from all handles and shutdown everything.

While connections are implicitly made upon first need, and thus are
invisible to the user, there are circumstances where it can be
important to call C<disconnect()> explicitly.

=head2 add

[$rc = ] add ($key, $value[, $exptime, $cb-E<gt>($rc) || $cv])

If the specified key does not already exist in the cache, it will be
set to the specified value.  If an expiration is included, it will
determine the lifetime of the object on the server.

If the add succeeds, 1 will be returned, if it fails, 0 will be
returned.

=head2 add_multi

[$rc = ] add_multi (\@([$key, $value, $exptime]), [$cb-E<gt>($rc) || $cv])

Given an arrayref of [key, value, $exptime] tuples, iterate over them
and if the specified key does not already exist in the cache, it will
be set to the specified value.  If an expiration is included, it will
determine the lifetime of the object on the server.

Returns a hashref of [key, boolean] tuples, where 1 means the add
succeeded, 0 means it failed.

=head2 append

[$rc = ] append ($key, $value[, $cb-E<gt>($rc) || $cv])

If the specified key already exists in the cache, it will have the
specified content appended to it.

If the append succeeds, 1 will be returned, if it fails, 0 will be
returned.

=head2 append_multi

[$rc = ] append_multi (\@([$key, $value, $exptime]), [$cb-E<gt>($rc) || $cv])

Given an arrayref of [key, value, $exptime] tuples, iterate over them
and if the specified key already exists in the cache, it will have the
the specified value appended to it.  If an expiration is included, it
will determine the lifetime of the object on the server.

Returns a hashref of [key, boolean] tuples, where 1 means the add
succeeded, 0 means it failed.

=head2 decr

[$value = ] decr ($key, [$delta (= 1), $initial, $cb-E<gt>($value) || $cv])

If the specified key already exists in the cache, it will be
decremented by the specified delta value, or 1 if no delta is
specified.

If the value does not exist in the cache, and an initial value is
supplied, the key will be set to that value.

If the decr succeeds, the resulting value will be returned, otherwise
undef will be the result.

=head2 decr_multi

[$value = ] decr_multi (\@($key, [$delta (= 1), $initial]), $cb-E<gt>($value) || $cv])

If the specified key already exists in the cache, it will be
decremented by the specified delta value, or 1 if no delta is
specified.

If the value does not exist in the cache, and an initial value is
supplied, the key will be set to that value.

If the decr succeeds, the resulting value will be returned, otherwise
undef will be the result.

=head2 delete

[$rc = ] delete ($key, [$cb-E<gt>($rc) || $cv])

If the specified key exists in the cache, it will be deleted.

If the delete succeeds, 1 will be returned, otherwise 0 will be the
result.

=head2 delete_multi

[\%keys = ] delete_multi (@keys, [$cb-E<gt>($rc) || $cv])

For each key specified, if the specified key exists in the cache, it
will be deleted.

If the delete succeeds, 1 will be returned, otherwise 0 will be the
result.

=head2 flush_all

[\%servers = ] flush_all ([$cb-E<gt>(\%servers) || $cv])

Clears the keys on each memcached server.

Returns a hashref indicating which servers the flush succeeded on.

=head2 get

[$value = ] get ($key, [$cb-E<gt>($value) || $cv])

Retrieves the specified key from the cache, otherwise returning undef.

=head2 get_multi

[\%values = ] get_multi (@values, [$cb-E<gt>(\%values) || $cv])

Retrieves the specified keys from the cache, returning a hashref of
key => value pairs.

=head2 incr

[$value = ] incr ($key, [$delta (= 1), $initial, $cb-E<gt>($value) || $cv])

If the specified key already exists in the cache, it will be
incremented by the specified delta value, or 1 if no delta is
specified.

If the value does not exist in the cache, and an initial value is
supplied, the key will be set to that value.

If the incr succeeds, the resulting value will be returned, otherwise
undef will be the result.

=head2 incr_multi

[$value = ] incr_multi (\@($key, [$delta (= 1), $initial]), $cb-E<gt>($value) || $cv])

If the specified key already exists in the cache, it will be
incremented by the specified delta value, or 1 if no delta is
specified.

If the value does not exist in the cache, and an initial value is
supplied, the key will be set to that value.

If the incr succeeds, the resulting value will be returned, otherwise
undef will be the result.

=head2 prepend($key, $value, $cb->($rc));

[$rc = ] append ($key, $value[, $cb-E<gt>($rc) || $cv])

If the specified key already exists in the cache, it will have the
specified content prepended to it.

If the prepend succeeds, 1 will be returned, if it fails, 0 will be
returned.

=head2 prepend_multi

[$rc = ] prepend_multi (\@([$key, $value, $exptime]), [$cb-E<gt>($rc) || $cv])

Given an arrayref of [key, value, $exptime] tuples, iterate over them
and if the specified key already exists in the cache, it will have the
the specified value prepended to it.  If an expiration is included, it
will determine the lifetime of the object on the server.

Returns a hashref of [key, boolean] tuples, where 1 means the add
succeeded, 0 means it failed.

=head2 remove

Alias to delete

=head2 replace

[$rc = ] replace ($key, $value[, $exptime, $cb-E<gt>($rc) || $cv])

If the specified key already exists in the cache, it will be replaced
by the specified value.  If it doesn't already exist, nothing will
happen.  If an expiration is included, it will determine the lifetime
of the object on the server.

If the replace succeeds, 1 will be returned, if it fails, 0 will be
returned.

=head2 replace_multi

[$rc = ] replace_multi (\@([$key, $value, $exptime]), [$cb-E<gt>($rc) || $cv])

Given an arrayref of [key, value, $exptime] tuples, iterate over them
and if the specified key already exists in the cache, it will be set
to the specified value.  If an expiration is included, it will
determine the lifetime of the object on the server.

Returns a hashref of [key, boolean] tuples, where 1 means the replace
succeeded, 0 means it failed.

=head2 set()

[$rc = ] set ($key, $value[, $exptime, $cb-E<gt>($rc) || $cv])

Set the specified key to the specified value.  If an expiration is
included, it will determine the lifetime of the object on the server.

If the set succeeds, 1 will be returned, if it fails, 0 will be
returned.

=head2 set_multi

[$rc = ] set_multi (\@([$key, $value, $exptime]), [$cb-E<gt>($rc) || $cv])

Given an arrayref of [key, value, $exptime] tuples, iterate over them
and set the specified key to the specified value.  If an expiration is
included, it will determine the lifetime of the object on the server.

Returns a hashref of [key, boolean] tuples, where 1 means the set
succeeded, 0 means it failed.

=head2 stats ()

[\%stats = ] stats ([$name, $cb-E<gt>(\%stats) || $cv])

Retrieves stats from all memcached servers.

Returns a hashref of hashrefs with the named stats.

=head2 version()

[\%versions = ] stats ([$cb-E<gt>(\%versions) || $cv])

Retrieves the version number from all memcached servers.

Returns a hashref of server => version pairs.

=head1 METHODS (INTERACTION)

All methods are intended to be called in either a synchronous or
asynchronous fashion.

A method is considered to have been called in a synchronous fashion if
it is provided without a callback as its last parameter.  Because of
the way the synchronous mode is implemented, it B<must not> be used
with programs that will call an event loop on their own (often by
calling C<-E<gt>recv> on a condvar)---you will likely get an error:

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

=head1 AUTHOR

Michael Alan Dorman <mdorman@ironicdesign.com>

=head1 COPYRIGHT AND LICENSE

This software is copyright (c) 2010 by Michael Alan Dorman.

This is free software; you can redistribute it and/or modify it under
the same terms as the Perl 5 programming language system itself.

=cut

