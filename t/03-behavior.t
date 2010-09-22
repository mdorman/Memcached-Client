#!/usr/bin/perl

use Memcached::Client qw{};
use Memcached::Client::Log qw{DEBUG INFO};
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

             ['add',
              '->add without a key'],
             ['add', 'foo',
              '->add without a value'],
             ['add', 'bar', 'baz',
              '->add with a value'],
             ['add', 'bar', 'foo',
              '->add with an existing value'],

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
             ['get_multi', ['bar', 'foo'],
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

             ['get', 'foo',
              '->get to verify ->prepend'],

             ['delete',
              '->delete without a key'],
             ['delete', 'bang',
              '->delete with a non-existent key'],
             ['delete', 'foo',
              '->delete with an existing key'],

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

             ['flush_all',
              '->flush_all to clear servers'],

             ['get_multi', ['bar', 'foo'],
              '->get with all keys set so far']);

if (0 == system (q{memcached -h})) {
    plan tests => (4 * (4 + scalar @tests));
} else {
    plan skip_all => 'No memcached found';
}

for my $async (0..1) {
    for my $protocol qw(Text Binary) {
        for my $selector qw(Traditional) {
            note sprintf "running %s/%s %s", $selector, $protocol, $async ? "asynchronous" : "synchronous";
            run ($async, $selector, $protocol);
        }
    }
}

sub run {
    my ($async, $selector, $protocol) = @_;

    DEBUG "T: running %s/%s %s", $selector, $protocol, $async ? "asynchronous" : "synchronous";

    isa_ok (my $servers = t::Memcached::Servers->new, 't::Memcached::Servers', 'Get memcached server list manager');
    my %args = (protocol => $protocol, selector => $selector, servers => $servers->servers);
    isa_ok (my $manager = t::Memcached::Manager->new (%args), 't::Memcached::Manager', 'Get memcached manager');
    $args{namespace} = join ('.', time, $$, '');
    isa_ok (my $client = Memcached::Client->new (%args), 'Memcached::Client', "Get memcached client");
    $args{version} = $manager->version;
    isa_ok (my $mock = t::Memcached::Mock->new (%args), 't::Memcached::Mock', "Get mock memcached client");

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
                is_deeply ($_[0], $expected, $msg) or DEBUG "T: %s - %s, received %s, expected %s", $msg, join (" - ", @{$tests[$n]}), $_[0], $expected;
            } else {
                is ($_[0], $expected, $msg) or DEBUG "T: %s - %s, received %s, expected %s", $msg, join (" - ", @{$tests[$n]}, $_[0]), $expected;
            }
            $cv->send if ($async and $n == $#tests)};
        if ($async) {
            $client->$method (@args, $test);
        } else {
            $test->($client->$method (@args));
        }

        if ($n == $fail) {
            my $candidate = $servers->error;
            DEBUG "T: Failing %s", $candidate;
            if ($async) {
                # To be able to get consistent results with the mock
                # object, we *must* sync on the cv here.
                $client->version (sub {
                                      note "Failing $candidate";
                                      $manager->error ($candidate);
                                      $mock->error ($candidate);
                                      $cv->send;
                                  });
                $cv->recv;
                $cv = AE::cv;
            } else {
                note "Failing $candidate";
                $mock->error ($candidate);
                $manager->error ($candidate);
            }
        }



    }

    $cv->recv if $async;

}

1;
