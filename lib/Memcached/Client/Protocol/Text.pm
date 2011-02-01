package Memcached::Client::Protocol::Text;
# ABSTRACT: Implements original text-based memcached protocol

use strict;
use warnings;
use Memcached::Client::Log qw{DEBUG LOG};
use base qw{Memcached::Client::Protocol};

sub __cmd {
    return join (' ', grep {defined} @_) . "\r\n";
}

sub __add {
    my ($self, $r, $c) = @_;
    #FIXME: @{$self}{qw{command data flags}} = $self->{client}->{compressor}->compress ($self->{client}->{serializer}->serialize ($self->{command}, $value));
    my $command = __cmd ($r->{command}, $r->{key}, $r->{flags}, $r->{expiration}, length $r->{data}) . __cmd ($r->{data});
    $self->rlog ($r, $c, $command) if DEBUG;
    $c->{handle}->push_write ($command);
    $c->{handle}->push_read (line => sub {
                                 my ($handle, $line) = @_;
                                 $r->result ($line eq 'STORED' ? 1 : 0);
                                 $c->complete;
                             });
}

sub __decr {
    my ($self, $r, $c) = @_;
    my $command = __cmd ($r->{command}, $r->{key}, $r->{delta});
    $self->rlog ($r, $c, $command) if DEBUG;
    $c->{handle}->push_write ($command);
    $c->{handle}->push_read (line => sub {
                                 my ($handle, $line) = @_;
                                 if ($line eq 'NOT_FOUND') {
                                     if ($r->{data}) {
                                            $command = __cmd (add => $r->{key}, 0, 0, length $r->{data}) . __cmd ($r->{data});
                                            $c->{handle}->push_write ($command);
                                            $c->{handle}->push_read (line => sub {
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
    my $command = __cmd (delete => $r->{key});
    $self->rlog ($r, $c, $command) if DEBUG;
    $c->{handle}->push_write ($command);
    $c->{handle}->push_read (line => sub {
                                 my ($handle, $line) = @_;
                                 $r->result ($line eq 'DELETED' ? 1 : 0);
                                 $c->complete;
                             });
}

sub __flush_all {
    my ($self, $r, $c) = @_;
    my $command = $r->{delay} ? __cmd (flush_all => $r->{delay}) : __cmd ("flush_all");
    $self->rlog ($r, $c, $command) if DEBUG;
    $c->{handle}->push_write ($command);
    $c->{handle}->push_read (line => sub {
                                 my ($handle, $line) = @_;
                                 $r->result (1);
                                 $c->complete;
                             });
}

sub __get {
    my ($self, $r, $c) = @_;
    my $command = __cmd (get => $r->{key});
    $self->rlog ($r, $c, $command) if DEBUG;
    $c->{handle}->push_write ($command);
    $c->{handle}->push_read (line => sub {
                                 my ($handle, $line) = @_;
                                 $self->log ("Got line %s", $line) if DEBUG;
                                 my @bits = split /\s+/, $line;
                                 if ($bits[0] eq "VALUE") {
                                     my ($key, $flags, $size, $cas) = @bits[1..4];
                                     $c->{handle}->unshift_read (chunk => $size, sub {
                                                                my ($handle, $data) = @_;
                                                                # Catch the \r\n trailing the value...
                                                                $c->{handle}->unshift_read (line => sub {
                                                                                           my ($handle, $line) = @_;
                                                                                           $c->{handle}->unshift_read (line => sub {
                                                                                                                      my ($handle, $line) = @_;
                                                                                                                      warn ("Unexpected result $line from $command") unless ($line eq 'END');
                                     # FIXME: $self->{result} = $self->{client}->{serializer}->deserialize ($self->{client}->{compressor}->decompress ($data, $flags));
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
    my $command = $r->{command} ? __cmd (stats => $r->{command}) : __cmd ("stats");
    $self->rlog ($r, $c, $command) if DEBUG;
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
    my $command = __cmd ("version");
    $self->rlog ($r, $c, $command) if DEBUG;
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

=method log

=cut

sub log {
    my ($self, $format, @args) = @_;
    #my $prefix = ref $self;
    #$prefix =~ s,Memcached::Client::Request::,Request/,;
    LOG ("Protocol> " . $format, @args);
}

=method rlog

=cut

sub rlog {
    my ($self, $request, $connection, $command) = @_;
    LOG ("Protocol/%s/%s> %s", $connection->{server}, join (" ", $request->{command}, $request->{key}), $command);
}

1;
