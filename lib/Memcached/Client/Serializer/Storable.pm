package Memcached::Client::Serializer::Storable;
#ABSTRACT: Implements Traditional Memcached Serializing (Storable and Gzip)

use bytes;
use strict;
use warnings;
use Memcached::Client::Log qw{DEBUG};
use Storable qw{};
use base qw{Memcached::Client::Serializer};

use constant +{
    HAVE_ZLIB => eval { require Compress::Zlib; 1 },
    F_STORABLE => 1,
    F_COMPRESS => 2,
    COMPRESS_SAVINGS => 0.20,
};

=method compress_threshold()

Retrieve or change the compress_threshold value.

=cut

sub compress_threshold {
    my ($self, $new) = @_;
    my $ret = $self->{compress_threshold};
    $self->{compress_threshold} = $new if ($new);
    return $ret;
}

sub deserialize {
    my ($self, $data, $flags) = @_;

    return unless ($data);

    $flags ||= 0;

    if ($flags & F_COMPRESS && HAVE_ZLIB) {
        DEBUG "Uncompressing data";
        $data = Compress::Zlib::memGunzip ($data);
    }
    if ($flags & F_STORABLE) {
        DEBUG "Thawing data";
        $data = Storable::thaw ($data);
    }

    return $data;
}

sub serialize {
    my ($self, $data, $command) = @_;

    return unless ($data);

    $command ||= '';

    my $flags = 0;

    if (ref $data) {
        DEBUG "Freezing data";
        $data = Storable::nfreeze ($data);
        $flags |= F_STORABLE;
    }

    my $len = bytes::length ($data);

    if (HAVE_ZLIB) {
        my $compressable = ($command ne 'append' && $command ne 'prepend') && $self->{compress_threshold} && $len >= $self->{compress_threshold};

        if ($compressable) {
            DEBUG "Compressing data";
            my $c_val = Compress::Zlib::memGzip ($data);
            my $c_len = bytes::length ($c_val);

            if ($c_len < $len * (1 - COMPRESS_SAVINGS)) {
                DEBUG "Compressing is a win";
                $data = $c_val;
                $flags |= F_COMPRESS;
                $len = $c_len;
            }
        }
    }

    return ($data, $flags, $len);
}

1;
