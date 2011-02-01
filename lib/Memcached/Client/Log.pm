package Memcached::Client::Log;
# ABSTRACT: Logging support for Memcached::Client

use strict;
use warnings;
use Data::Dumper qw{Dumper};
use IO::File qw{};
use Scalar::Util qw{blessed};
use base qw{Exporter};

our @EXPORT = qw{DEBUG LOG};

=head1 SYNOPSIS

  package Memcached::Client::Log;
  DEBUG "This is a structure: %s", \%foo;

=method DEBUG

When the environment variable MCDEBUG is true, DEBUG() will warn the
user with the specified message, formatted with sprintf and dumping
the structure of any references that are made.

If the variable MCDEBUG is false, the debugging code should be
compiled out entirely.

=method LOG

LOG() will inform the user with the specified message, formatted with
sprintf and dumping the structure of any references that are made.

=cut

# Hook into $SIG{__WARN__} if you want to route these debug messages
# into your own logging system.
BEGIN {
    my $log;

    if (exists $ENV{MCTEST} and $ENV{MCTEST}) {
        $ENV{MCDEBUG} = 1;
        open $log, ">>", ",,debug.log" or die "Couldn't open ,,debug.log";
        $log->autoflush (1);
    }

    if ($ENV{MCDEBUG}) {
        *DEBUG = sub () {1};
    } else {
        *DEBUG = sub () {0};
    }

    *LOG = sub (@) {
        local *__ANON__ = "Memcached::Client::Log";
        local $Data::Dumper::Indent = 1;
        local $Data::Dumper::Quotekeys = 0;
        local $Data::Dumper::Sortkeys = 1;
        local $Data::Dumper::Terse = 1;
        my $format = shift or return;
        chomp (my $entry = @_ ? sprintf $format, map {
            if (defined $_) {
                if (ref $_) {
                    if (blessed $_ and $_->can ('as_string')) {
                        $_->as_string;
                    } else {
                        Dumper $_;
                    }
                } else {
                    $_
                }
            } else {
                '[undef]'
            }
        } @_ : $format);
        # my $output = "$callerinfo[3] $entry\n";
        my $output = "$entry\n";
        if ($ENV{MCTEST}) {
            $log->print ($output);
        } else {
            warn $output;
        }
    };
}

1;
