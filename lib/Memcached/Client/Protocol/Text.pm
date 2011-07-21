package Memcached::Client::Protocol::Text;
BEGIN {
  $Memcached::Client::Protocol::Text::VERSION = '1.05';
}
# ABSTRACT: Implements original text-based memcached protocol

use strict;
use warnings;
use Memcached::Client::Log qw{DEBUG};
use base qw{Memcached::Client::Protocol};

sub __cmd {
    return join (' ', grep {defined} @_) . "\r\n";
}

{
    my $generator = sub {
        my ($name) = @_;
        sub {
            my ($self, $handle, $cv, $key, $value, $flags, $expiration) = @_;
            DEBUG "P: %s: %s - %s - %s", $name, $handle->{peername}, $key, $value;
            my $command = __cmd ($name, $key, $flags, $expiration, length $value) . __cmd ($value);
            $handle->push_write ($command);
            $handle->push_read (line => sub {
                                    my ($handle, $line) = @_;
                                    DEBUG "P [%s]: < %s", $handle->{peername}, $line;
                                    $cv->send ($line eq 'STORED' ? 1 : 0);
                                });
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
        my ($name) = @_;
        return sub {
            my ($self, $handle, $cv, $key, $delta, $initial) = @_;
            DEBUG "P: %s: %s - %s - %s", $name, $handle->{peername}, $key, $delta;
            my $command = __cmd ($name, $key, $delta);
            DEBUG "P [%s]: > %s", $handle->{peername}, $command;
            $handle->push_write ($command);
            $handle->push_read (line => sub {
                                    my ($handle, $line) = @_;
                                    DEBUG "P [%s]: < %s", $handle->{peername}, $line;
                                    if ($line eq 'NOT_FOUND') {
                                        if ($initial) {
                                            $command = __cmd (add => $key, 0, 0, length $initial) . __cmd ($initial);
                                            $handle->push_write ($command);
                                            DEBUG "P [%s]: > %s", $handle->{peername}, $command;
                                            $handle->push_read (line => sub {
                                                                    my ($handle, $line) = @_;
                                                                    DEBUG "P [%s]: < %s", $handle->{peername}, $line;
                                                                    $cv->send ($line eq 'STORED' ? $initial : undef);
                                                                });
                                        } else {
                                            $cv->send;
                                        }
                                    } else {
                                        $cv->send ($line);
                                    }
                                });
        }
    };

    *__decr = $generator->("decr");
    *__incr = $generator->("incr");
}

sub __delete {
    my ($self, $handle, $cv, $key) = @_;
    DEBUG "P: delete: %s - %s", $handle->{peername}, $key;
    my $command = __cmd (delete => $key);
    $handle->push_write ($command);
    DEBUG "P [%s]: > %s", $handle->{peername}, $command;
    $handle->push_read (line => sub {
                            my ($handle, $line) = @_;
                            DEBUG "P [%s]: < %s", $handle->{peername}, $line;
                            $cv->send ($line eq 'DELETED' ? 1 : 0);
                        });
}

sub __flush_all {
    my ($self, $handle, $cv, $delay) = @_;
    my $command = $delay ? __cmd (flush_all => $delay) : __cmd ("flush_all");
    $handle->push_write ($command);
    DEBUG "P: flush_all: %s", $handle->{peername};
    DEBUG "P [%s]: > %s", $handle->{peername}, $command;
    $handle->push_read (line => sub {
                            my ($handle, $line) = @_;
                            DEBUG "P [%s]: < %s", $handle->{peername}, $line;
                            $cv->send (1);
                        });
}

sub __get {
    my ($self, $handle, $cv, @keys) = @_;
    for my $key (@keys) {
        DEBUG "P: get: %s - %s", $handle->{peername}, $key;
    }
    my $command = __cmd (get => @keys);
    $handle->push_write ($command);
    my ($result);
    my $code; $code = sub {
        my ($handle, $line) = @_;
        DEBUG "P [%s]: < %s", $handle->{peername}, $line;
        my @bits = split /\s+/, $line;
        if ($bits[0] eq "VALUE") {
            my ($key, $flags, $size, $cas) = @bits[1..4];
            $handle->unshift_read (chunk => $size, sub {
                                       my ($handle, $data) = @_;
                                       DEBUG "P [%s]: < %s", $handle->{peername}, $data;
                                       $result->{$key} = {cas => $cas, data => $data, flags => $flags};
                                       # Catch the \r\n trailing the value...
                                       $handle->unshift_read (line => sub {
                                                                  my ($handle, $line) = @_;
                                                                  DEBUG "P [%s]: < %s", $handle->{peername}, $line;
                                                              });
                                       # ...and then start looking for another line
                                       $handle->push_read (line => $code);
                                   });
        } else {
            warn ("Unexpected result $line from $command") unless ($bits[0] eq 'END');
            undef $code;
            $cv->send ($result);
        }
    };
    $handle->push_read (line => $code);
}

sub __stats {
    my ($self, $handle, $cv, $name) = @_;
    my $command = $name ? __cmd (stats => $name) : __cmd ("stats");
    $handle->push_write ($command);
    DEBUG "P: stats: %s", $handle->{peername};
    DEBUG "P [%s]: > %s", $handle->{peername}, $command;
    my ($result);
    my $code; $code = sub {
        my ($handle, $line) = @_;
        DEBUG "P [%s]: < %s", $handle->{peername}, $line;
        my @bits = split /\s+/, $line;
        if ($bits[0] eq 'STAT') {
            $result->{$bits[1]} = $bits[2];
            $handle->push_read (line => $code);
        } else {
            warn ("Unexpected result $line from $command") unless ($bits[0] eq 'END');
            undef $code;
            $cv->send ($result);
        }
    };
    $handle->push_read (line => $code);
}

sub __version {
    my ($self, $handle, $cv) = @_;
    my $command = __cmd ("version");
    $handle->push_write ($command);
    DEBUG "P: version: %s", $handle->{peername};
    DEBUG "P [%s]: > %s", $handle->{peername}, $command;
    $handle->push_read (line => sub {
                            my ($handle, $line) = @_;
                            DEBUG "P [%s]: < %s", $handle->{peername}, $line;
                            my @bits = split /\s+/, $line;
                            if ($bits[0] eq 'VERSION') {
                                $cv->send ($bits[1]);
                            } else {
                                warn ("Unexpected result $line from $command");
                                $cv->send;
                            }
                        });
}

1;

__END__
=pod

=head1 NAME

Memcached::Client::Protocol::Text - Implements original text-based memcached protocol

=head1 VERSION

version 1.05

=head1 AUTHOR

Michael Alan Dorman <mdorman@ironicdesign.com>

=head1 COPYRIGHT AND LICENSE

This software is copyright (c) 2010 by Michael Alan Dorman.

This is free software; you can redistribute it and/or modify it under
the same terms as the Perl 5 programming language system itself.

=cut

