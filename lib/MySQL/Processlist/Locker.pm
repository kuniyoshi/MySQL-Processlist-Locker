use strict;
use warnings;
package MySQL::Processlist::Locker;
use Data::Dumper;
use Readonly;
use List::Util qw( max );
use MySQL::Processlist::Locker::Filter;

# ABSTRACT: detect locker sql on mysql from show processlist.

our $VERSION = "0.000";

Readonly my %DEFAULT => (
    max_iteration  => 2,
    interval       => 5,
    time_threshold => 5,
    lock_threshold => 10,
    verbose        => 0,
    stream         => "stdin",
    detected_at    => \&__dump_lockers,
);
Readonly my %STREAM => (
    dbi   => join( q{::}, __PACKAGE__, "Stream", "Dbi" ),
    stdin => join( q{::}, __PACKAGE__, "Stream", "Stdin" ),
);
Readonly my $DEBUG => !!$ENV{MYSQL_PROCESS_LIST_LOCKER_DEBUG};

sub __dump_lockers {
    my @lockers = @_;

    for my $locker_ref ( @lockers ) {
        warn Data::Dumper->new( [ $locker_ref ] )->Terse( 1 )->Sortkeys( 1 )->Useqq( 1 )->Indent( 0 )->Dump;
    }
}

sub new {
    my $class = shift;
    my %param = ( %DEFAULT, @_ );
    my $self = bless \%param, $class;
    return $self;
}

sub stream {
    my $self = shift;
    my $class = $STREAM{ $self->{stream} };
    require $class;
    my $stream = $class->new( %{ $self } );
    return $stream;
}

sub detected_at {
    my $self    = shift;
    my @lockers = @_;
    $self->{detected_at}->( @lockers );
}

sub loop {
    my $self = shift;
    my $max_iteration  = $self->{max_iteration};
    my $time_threshold = $self->{time_threshold};
    my $lock_threshold = $self->{lock_threshold};
    my $interval       = $self->{interval};

    my $stream = $self->stream;
    $stream->prepare;
    my $count;
    my %process;

    local $| = 1;
    my $can_continue = 1;

    $SIG{INT} = sub { undef $can_continue };

    while ( $can_continue && $count++ < $max_iteration ) {
        $stream->execute( $time_threshold );
        my $lock_count      = 0;
        my $max_locked_time = 0;
        my @ids;

        while ( my $process_ref = $stream->fetch ) {
            if ( $process_ref->{STATE} eq "Locked" ) {
                $lock_count++;
                $max_locked_time = max( $process_ref->{TIME}, $max_locked_time );
            }

            my $id = $process_ref->{ID};
            push @ids, $id;

            next
                if $process_ref->{STATE} eq "Locked";

            if ( !exists $process{ $id } ) {
                $process{ $id } = MySQL::Processlist::Locker::Filter->new(
                    %{ $process_ref },
                    lock_threshold => $lock_threshold,
                    time_threshold => $time_threshold,
                    locks          => 0,
                );
            }
            else {
                @{ $process{ $id } }{ qw( INFO STATE TIME ) } = @{ $process_ref }{ qw( INFO STATE TIME ) };
            }
        }

        for my $process_ref ( values %process ) {
            next
                if !grep { $_ == $process_ref->{ID} } @ids;
            next
                if $process_ref->{TIME} < $max_locked_time;

            $process_ref->{locks}           = $lock_count;
            $process_ref->{max_locked_time} = $max_locked_time;
        }

        delete @process{ grep { my $id = $_; !grep { $_ == $id } @ids } keys %process };

        if ( my @lockers = MySQL::Processlist::Locker::Filter::flush( ) ) {
            $self->detected_at( @lockers );
        }

        sleep $interval;
    }
}

1;

__END__
{
  'COMMAND' => 'Connect',
  'DB' => undef,
  'HOST' => '',
  'ID' => '667490',
  'INFO' => undef,
  'STATE' => 'Waiting for master to send event',
  'TIME' => '39812',
  'USER' => 'system user'
}
