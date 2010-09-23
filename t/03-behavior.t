#!/usr/bin/perl

use Memcached::Client qw{};
use Memcached::Client::Log qw{DEBUG INFO};
use Storable qw{freeze thaw};
use t::Memcached::Manager qw{};
use t::Memcached::Mock qw{};
use t::Memcached::Servers qw{};
use Test::More;

my @tests = (['version',
              'Checking for version on all servers'],

             ['set',
              '->set without a key'],
             ['set', 'foo',
              '->set without a value'],
             ['set', 'foo', 'bar',
              '->set with a value'],
             ['set', ['37', 'llama'], 'bar',
              '->set with a pre-hashed key'],
             ['set_multi', [['teatime', 3], ['bagman', 'ludo']],
              '->set_multi with various keys'],

             ['add',
              '->add without a key'],
             ['add', 'foo',
              '->add without a value'],
             ['add', 'bar', 'baz',
              '->add with a value'],
             ['add', 'bar', 'foo',
              '->add with an existing value'],
             ['add_multi', [['teatime', 3], ['bagman', 'ludo']],
              '->set_multi with various pre-existing keys'],
             ['add_multi', [['porridge', 'salty'], ['complex', 'simple'], ['bagman', 'horace']],
              '->set_multi with various keys'],

             ['set', ['19', 'ding-dong'], 'bar',
              '->add with a pre-hashed key'],

             ['get',
              '->get without a key'],
             ['get', 'bang',
              '->get a non-existant value'],
             ['get', 'bar',
              '->get an existing value'],

             ['get', ['19', 'ding-dong'],
              '->get a value with a pre-hashed key'],

             ['get_multi',
              '->get_multi without a list'],
             ['get_multi', [],
              '->get_multi with an empty list'],
             ['get_multi', ['bar', 'foo', 'porridge'],
              '->get with all keys set so far'],

             ['get_multi', [['37', 'llama'], 'bar', 'foo'],
              '->get with all keys set so far'],

             ['replace',
              '->replace without a key'],
             ['replace', 'foo',
              '->replace without a value'],
             ['replace', 'baz', 'gorp',
              '->replace with a non-existent value'],
             ['replace', 'bar', 'gondola',
              '->replace with an existing value'],
             ['replace_multi', [['porridge', 'sweet'], ['complex', 'NP'], ['ludo', 'panopticon']],
              '->replace_multi with various keys'],

             ['get', 'bar',
              '->get to verify replacement'],

             ['replace', ['18', 'ding-dong'], 'bar',
              '->replace with a pre-hashed key and non-existent value'],
             ['replace', ['19', 'ding-dong'], 'baz',
              '->replace with a pre-hashed key and an existing value'],
             ['get', ['19', 'ding-dong'],
              '->get a value with a pre-hashed key'],

             ['append',
              '->append without a key'],
             ['append', 'foo',
              '->append without a value'],
             ['append', 'baz', 'gorp',
              '->append with a non-existent value'],
             ['append', 'bar', 'gorp',
              '->append with an existing value'],
             ['append_multi', [['porridge', ' and salty'], ['complex', ' != P']],
              '->append_multi with various keys'],

             ['get', 'bar',
              '->get to verify ->append'],

             ['append', ['18', 'ding-dong'], 'flagon',
              '->append with a pre-hashed key and non-existent value'],
             ['append', ['19', 'ding-dong'], 'flagged',
              '->append with a pre-hashed key and an existing value'],
             ['get', ['19', 'ding-dong'],
              '->get a value with a pre-hashed key'],

             ['prepend',
              '->prepend without a key'],
             ['prepend', 'foo',
              '->prepend without a value'],
             ['prepend', 'baz', 'gorp',
              '->prepend with a non-existent value'],
             ['prepend', 'foo', 'gorp',
              '->prepend with an existing value'],
             ['prepend_multi', [['porridge', 'We love ']],
              '->prepend_multi with various keys'],


             ['get', 'foo',
              '->get to verify ->prepend'],

             ['delete',
              '->delete without a key'],
             ['delete', 'bang',
              '->delete with a non-existent key'],
             ['delete', 'foo',
              '->delete with an existing key'],
             ['delete_multi', 'complex', 'panopticon',
              '->delete_multi with various keys'],

             ['get', 'foo',
              '->get to verify ->delete'],

             ['add', 'foo', '1',
              '->add with a value'],
             ['get', 'foo',
              '->get to verify ->add'],

             ['incr',
              '->incr without a key'],
             ['incr', 'bang',
              '->incr with a non-existent key'],
             ['incr', 'foo',
              '->incr with an existing key'],
             ['incr', 'foo', '72',
              '->incr with an existing key and an amount'],
             ['get', 'foo',
              '->get to verify ->incr'],

             ['decr',
              '->decr without a key'],
             ['decr', 'bang',
              '->decr with a non-existent key'],
             ['decr', 'foo',
              '->decr with an existing key'],
             ['decr', 'foo', 18,
              '->decr with an existing key'],
             ['get', 'foo',
              '->get to verify ->decr'],

             ['get_multi', ['bar', 'foo'],
              '->get with all keys set so far'],

             ['incr_multi', [['foo']],
              '->incr_multi with various keys'],

             ['incr_multi', [['braga', 1, 17], ['foo', 7]],
              '->incr_multi with various keys'],

             ['decr_multi', [['braga', 3], ['bartinate', 7, 33]],
              '->decr_multi with various keys'],

             ['flush_all',
              '->flush_all to clear servers'],

             ['get_multi', ['bar', 'foo'],
              '->get with all keys set so far']);

my $memcached = $ENV{MEMCACHED} || qx{which memcached};

chomp ($memcached);

if ($memcached) {
    plan tests => 2 + (4 * (scalar @tests + 2));
} else {
    plan skip_all => 'No memcached found';
}

isa_ok (my $servers = t::Memcached::Servers->new, 't::Memcached::Servers', 'Get memcached server list manager');
isa_ok (my $manager = t::Memcached::Manager->new (memcached => $memcached, servers => $servers->servers), 't::Memcached::Manager', 'Get memcached manager');

for my $async (0..1) {
    for my $protocol qw(Text Binary) {
        for my $selector qw(Traditional) {
            note sprintf "running %s/%s %s", $selector, $protocol, $async ? "asynchronous" : "synchronous";
            my $namespace = join ('.', time, $$, '');
            isa_ok (my $client = Memcached::Client->new (namespace => $namespace, protocol => $protocol, selector => $selector, servers => $servers->servers), 'Memcached::Client', "Get memcached client");
            isa_ok (my $mock = t::Memcached::Mock->new (namespace => $namespace, selector => $selector, servers => $servers->servers, version => $manager->version), 't::Memcached::Mock', "Get mock memcached client");
            my $candidate = $servers->error;
            my $tests = freeze \@tests;
            run ($async, $selector, $protocol, $candidate, $client, $mock, $tests);
            if ($ENV{FAIL_TEST}) {
                # Restart the failed server
                $manager->start ($candidate);
                $mock->start ($candidate);
            }
        }
    }
}

sub run {
    my ($async, $selector, $protocol, $candidate, $client, $mock, $tests) = @_;

    my @tests = @{thaw $tests};

    # DEBUG "T: running %s/%s %s", $selector, $protocol, $async ? "asynchronous" : "synchronous";

    my $fail = int (rand ($#tests - 10));

    my $cv = AE::cv if $async;

    for my $n (0..$#tests) {
        my @args = @{$tests[$n]};
        my $method = shift @args;
        my $msg = pop @args;

        my $expected = $mock->$method (@args);
        my $test = sub {
            my ($received) = @_;
            if (ref $expected) {
                is_deeply ($_[0], $expected, $msg) or DEBUG ("T: %s - %s, received %s, expected %s, mock %s", $msg, join (" - ", @{$tests[$n]}), $_[0], $expected, $mock), BAIL_OUT;
            } else {
                is ($_[0], $expected, $msg) or DEBUG ("T: %s - %s, received %s, expected %s, mock %s", $msg, join (" - ", @{$tests[$n]}, $_[0]), $expected, $mock), BAIL_OUT;
            }
            $cv->send if ($async and $n == $#tests)};
        if ($async) {
            $client->$method (@args, $test);
        } else {
            $test->($client->$method (@args));
        }

        next unless $ENV{FAIL_TEST};

        if ($n == $fail) {
            # DEBUG "T: Failing %s", $candidate;
            if ($async) {
                # To be able to get consistent results with the mock
                # object, we *must* sync on the cv here.
                $client->version (sub {
                                      note "Failing $candidate";
                                      $manager->stop ($candidate);
                                      $mock->stop ($candidate);
                                      $cv->send;
                                  });
                $cv->recv;
                $cv = AE::cv;
            } else {
                note "Failing $candidate";
                $mock->stop ($candidate);
                $manager->stop ($candidate);
            }
        }
    }

    $cv->recv if $async;
}

1;
