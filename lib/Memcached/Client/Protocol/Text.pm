package Memcached::Client::Protocol::Text;
# ABSTRACT: Implements original text-based memcached protocol

use strict;
use warnings;
use Memcached::Client::Log qw{DEBUG};
use base qw{Memcached::Client::Protocol};

sub __cmd {
    my $command = join (' ', grep {defined} @_);
    DEBUG $command;
    return "$command\r\n";
}

sub __add {
    my ($self, $r, $c) = @_;
    DEBUG "%s: %s - %s - %s", $r->{command}, $c->{server}, $r->{key}, $r->{data};
    my $command = __cmd ($r->{command}, $r->{key}, $r->{flags}, $r->{expiration}, length $r->{data}) . __cmd ($r->{data});
    $c->{handle}->push_write ($command);
    $c->{handle}->push_read (line => sub {
                                 my ($handle, $line) = @_;
                                 $r->result ($line eq 'STORED' ? 1 : 0);
                                 $c->complete;
                             });
}

sub __decr {
    my ($self, $r, $c) = @_;
    DEBUG "%s: %s - %s - %s", $r->{command}, $c->{server}, $r->{key}, $r->{delta};
    my $command = __cmd ($r->{command}, $r->{key}, $r->{delta});
    $c->{handle}->push_write ($command);
    $c->{handle}->push_read (line => sub {
                                 my ($handle, $line) = @_;
                                 if ($line eq 'NOT_FOUND') {
                                     if ($r->{data}) {
                                            $command = __cmd (add => $r->{key}, 0, 0, length $r->{data}) . __cmd ($r->{data});
                                            $handle->push_write ($command);
                                            $handle->push_read (line => sub {
                                                                    my ($handle, $line) = @_;
                                                                    $r->result ($line eq 'STORED' ? $r->{data} : undef);
                                                                    $c->complete;
                                                                });
                                     } else {
                                         $r->result;
                                         $c->complete
                                     }
                                 } else {
                                     $r->result ($line);
                                     $c->complete;
                                 }
                             });
}

sub __delete {
    my ($self, $r, $c) = @_;
    DEBUG "delete: %s - %s", $c->{server}, $r->{key};
    my $command = __cmd (delete => $r->{key});
    $c->{handle}->push_write ($command);
    $c->{handle}->push_read (line => sub {
                                 my ($handle, $line) = @_;
                                 $r->result ($line eq 'DELETED' ? 1 : 0);
                                 $c->complete;
                             });
}

sub __flush_all {
    my ($self, $r, $c) = @_;
    DEBUG "flush_all: %s", $c->{server};
    my $command = $r->{delay} ? __cmd (flush_all => $r->{delay}) : __cmd ("flush_all");
    $c->{handle}->push_write ($command);
    $c->{handle}->push_read (line => sub {
                                 my ($handle, $line) = @_;
                                 $r->result (1);
                                 $c->complete;
                             });
}

sub __get {
    my ($self, $r, $c) = @_;
    DEBUG "get: %s - %s", $c->{server}, $r->{key};
    my $command = __cmd (get => $r->{key});
    DEBUG "Command %s", $command;
    $c->{handle}->push_write ($command);
    $c->{handle}->push_read (line => sub {
                                 my ($handle, $line) = @_;
                                 DEBUG "Got line %s", $line;
                                 my @bits = split /\s+/, $line;
                                 if ($bits[0] eq "VALUE") {
                                     my ($key, $flags, $size, $cas) = @bits[1..4];
                                     $handle->unshift_read (chunk => $size, sub {
                                                                my ($handle, $data) = @_;
                                                                # Catch the \r\n trailing the value...
                                                                $handle->unshift_read (line => sub {
                                                                                           my ($handle, $line) = @_;
                                                                                           $handle->unshift_read (line => sub {
                                                                                                                      my ($handle, $line) = @_;
                                                                                                                      warn ("Unexpected result $line from $command") unless ($line eq 'END');
                                                                                                                      $r->result ($data, $flags, $cas);
                                                                                                                      $c->complete;
                                                                                                                  });
                                                                                       });
                                                            });
                                 } elsif ($bits[0] eq "END") {
                                     $r->result;
                                     $c->complete;
                                 }
                             });
}

sub __stats {
    my ($self, $r, $c) = @_;
    DEBUG "P: stats: %s", $c->{server};
    my $command = $r->{command} ? __cmd (stats => $r->{command}) : __cmd ("stats");
    $c->{handle}->push_write ($command);
    my ($code, $result);
    $code = sub {
        my ($handle, $line) = @_;
        my @bits = split /\s+/, $line;
        if ($bits[0] eq 'STAT') {
            $result->{$bits[1]} = $bits[2];
            $c->{handle}->push_read (line => $code);
        } else {
            warn ("Unexpected result $line from $command") unless ($bits[0] eq 'END');
            undef $code;
            $r->result ($result);
            $c->complete;
        }
    };
    $c->{handle}->push_read (line => $code);
}

sub __version {
    my ($self, $r, $c) = @_;
    DEBUG "P: version: %s", $c->{server};
    my $command = __cmd ("version");
    $c->{handle}->push_write ($command);
    $c->{handle}->push_read (line => sub {
                                 my ($handle, $line) = @_;
                                 my @bits = split /\s+/, $line;
                                 if ($bits[0] eq 'VERSION') {
                                     $r->result ($bits[1]);
                                 } else {
                                     warn ("Unexpected result $line from $command");
                                 }
                                 $c->complete;
                             });
}

1;
