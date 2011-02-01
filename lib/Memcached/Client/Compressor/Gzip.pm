package Memcached::Client::Compressor::Gzip;
#ABSTRACT: Implements Memcached Compression using Gzip

use bytes;
use strict;
use warnings;
use Memcached::Client::Log qw{DEBUG};
use base qw{Memcached::Client::Compressor};

use constant +{
    HAVE_ZLIB => eval { require Compress::Zlib; 1 },
    F_COMPRESS => 2,
    COMPRESS_SAVINGS => 0.20
};

sub decompress {
    my ($self, $data, $flags) = @_;

    return unless defined $data;

    $flags ||= 0;

    if ($flags & F_COMPRESS && HAVE_ZLIB) {
        $self->log ("Uncompressing data") if DEBUG;
        $data = Compress::Zlib::memGunzip ($data);
    }

    return ($data, $flags);
}

sub compress {
    my ($self, $command, $data, $flags) = @_;

    $self->log ("Entering compress") if DEBUG;
    return unless defined $data;

    $self->log ("Have data") if DEBUG;
    my $len = bytes::length ($data);

    $self->log ("Checking for Zlib") if DEBUG;

    if (HAVE_ZLIB) {

        $self->log ("Checking for compressable (threshold $self->{compress_threshold}, command $command)") if DEBUG;
        my $compressable = ($command ne 'append' && $command ne 'prepend') && $self->{compress_threshold} && $len >= $self->{compress_threshold};

        if ($compressable) {
            $self->log ("Compressing data") if DEBUG;
            my $c_val = Compress::Zlib::memGzip ($data);
            my $c_len = bytes::length ($c_val);

            if ($c_len < $len * (1 - COMPRESS_SAVINGS)) {
                $self->log ("Compressing is a win") if DEBUG;
                $data = $c_val;
                $flags |= F_COMPRESS;
            }
        }
    }

    return ($command, $data, $flags);
}

1;
