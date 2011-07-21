package Memcached::Client::Log;
BEGIN {
  $Memcached::Client::Log::VERSION = '1.00';
}
# ABSTRACT: Logging support for Memcached::Client

use strict;
use warnings;
use Data::Dumper qw{Dumper};
use IO::File qw{};
use base qw{Exporter};

our @EXPORT = qw{DEBUG INFO};


# Hook into $SIG{__WARN__} if you want to route these debug messages
# into your own logging system.
BEGIN {
    my $log;

    if (exists $ENV{MCTEST} and $ENV{MCTEST}) {
        $ENV{MCDEBUG} = 1;
        open $log, "+>>,,debug.log" or die "Couldn't open ,,debug.log";
        $log->autoflush (1);
    }

    *INFO = sub (@) {
        local $Data::Dumper::Indent = 1;
        local $Data::Dumper::Quotekeys = 0;
        local $Data::Dumper::Sortkeys = 1;
        local $Data::Dumper::Terse = 1;
        my $format = shift or return;
        chomp (my $entry = @_ ? sprintf $format, map { defined $_ ? ref $_ ? Dumper $_ : $_ : '[undef]' } @_ : $format);
        if ($ENV{MCTEST}) {
            $log->print ("$entry\n");
        } else {
            warn "$entry\n";
        }
    };

    if ($ENV{MCDEBUG}) {
        *DEBUG = *INFO;
    } else {
        *DEBUG = sub (@) {};
    }
}

1;

__END__
=pod

=head1 NAME

Memcached::Client::Log - Logging support for Memcached::Client

=head1 VERSION

version 1.00

=head1 SYNOPSIS

  package Memcached::Client::Log;
  DEBUG "This is a structure: %s", \%foo;

=head1 METHODS

=head2 DEBUG

When the environment variable MCDEBUG is true, DEBUG() will warn the
user with the specified message, formatted with sprintf and dumping
the structure of any references that are made.

If the variable MCDEBUG is false, the debugging code should be
compiled out entirely.

=head2 INFO

INFO() will warn the user with the specified message, formatted with
sprintf and dumping the structure of any references that are made.

=head1 AUTHOR

Michael Alan Dorman <mdorman@ironicdesign.com>

=head1 COPYRIGHT AND LICENSE

This software is copyright (c) 2010 by Michael Alan Dorman.

This is free software; you can redistribute it and/or modify it under
the same terms as the Perl 5 programming language system itself.

=cut

