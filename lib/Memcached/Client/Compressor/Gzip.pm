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
    my ($self, $tuple) = @_;

    return unless defined $tuple->{data};

    $tuple->{flags} ||= 0;

    if ($tuple->{flags} & F_COMPRESS && HAVE_ZLIB) {
        DEBUG "Uncompressing data";
        $tuple->{data} = Compress::Zlib::memGunzip ($tuple->{data});
    }

    return $tuple;
}

sub compress {
    my ($self, $tuple, $command) = @_;

    DEBUG "Entering compress";
    return unless defined $tuple->{data};

    DEBUG "Have data";
    my $len = bytes::length ($tuple->{data});

    DEBUG "Checking for Zlib";

    if (HAVE_ZLIB) {

        DEBUG "Checking for compressable (threshold $self->{compress_threshold}, command $command)";
        my $compressable = ($command ne 'append' && $command ne 'prepend') && $self->{compress_threshold} && $len >= $self->{compress_threshold};

        if ($compressable) {
            DEBUG "Compressing data";
            my $c_val = Compress::Zlib::memGzip ($tuple->{data});
            my $c_len = bytes::length ($c_val);

            if ($c_len < $len * (1 - COMPRESS_SAVINGS)) {
                DEBUG "Compressing is a win";
                $tuple->{data} = $c_val;
                $tuple->{flags} |= F_COMPRESS;
            }
        }
    }

    return $tuple;
}

1;
