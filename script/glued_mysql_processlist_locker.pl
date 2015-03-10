#!/usr/bin/perl -s
use 5.10.0;
use strict;
use warnings;

use strict;
use warnings;
package MySQL::Processlist::Locker::Stream::Dbi;
use DBI;
use Carp ( );
use List::Util qw( first );

our $VERSION = "0.000";

sub new {
    my $class = shift;
    my %param = @_;

    my $dbh = DBI->connect(
        "dbi:mysql:information_schema",
        $param{username} || "root",
        $param{password} || q{},
        { RaiseError => 1, mysql_enable_utf8 => 1, %{ $param{attr} || { } } },
    )
        or die "Could not connect to db: $DBI::errstr";

    my $self = bless { dbh => $dbh }, $class;

    return $self;
}

sub has_mysql_processlist_table {
    my $self = shift;

    if ( exists $self->{_has_mysql_processlist_table} ) {
        return $self->{_has_mysql_processlist_table};
    }

    ( my $sth = $self->{dbh}->prepare( "SHOW TABLES" ) )->execute;
    my @tables = map { $_->[0] } @{ $sth->fetchall_arrayref };

    return $self->{_has_mysql_processlist_table} = first { uc( $_ ) eq "PROCESSLIST" } @tables;
}

sub prepare_table {
    my $self = shift;
    my $sth  = $self->{dbh}->prepare( <<END_SQL );
SELECT *
FROM PROCESSLIST
WHERE COMMAND != ? AND COMMAND != ? AND TIME > ?
END_SQL
    $self->{sth} = $sth;
}

sub prepare_command {
    my $self = shift;
    my $sth  = $self->{dbh}->prepare( "SHOW FULL PROCESSLIST" );
    $self->{sth} = $sth;
}

sub prepare {
    my $self = shift;

    if ( $self->has_mysql_processlist_table ) {
        return $self->prepare_table;
    }
    else {
        return $self->prepare_command;
    }
}

sub execute_table {
    my $self           = shift;
    my $time_threshold = shift
        or Carp::croak( "time_threshold required." );
    $self->{sth}->execute( "Sleep", "Connect", $time_threshold );
}

sub execute_command {
    my $self = shift;
    $self->{sth}->execute;
}

sub execute {
    my $self = shift;

    if ( $self->has_mysql_processlist_table ) {
        return $self->execute_table( @_ );
    }
    else {
        return $self->execute_command( @_ );
    }
}

sub __remap_to_uc_keys {
    my %hash = @_
        or return;
    @hash{ map { uc } keys %hash } = values %hash;
    delete @hash{ grep { m{[a-z]} } keys %hash };
    return %hash;
}

sub fetch_table {
    my $self = shift;
    my $href = $self->{sth}->fetchrow_hashref;
    return
        unless $href;
    $href = { __remap_to_uc_keys( %{ $href } ) };
    return $href;
}

sub fetch_command {
    my $self = shift;
    my $row_ref;

    while ( !$row_ref && ( my $hashref = $self->{sth}->fetchrow_hashref ) ) {
        $hashref = { __remap_to_uc_keys( %{ $hashref } ) };

        next
            if first { $_ eq $hashref->{COMMAND} } qw( Sleep Connect );
        next
            if $hashref->{COMMAND} eq "Query" && $hashref->{INFO} && $hashref->{INFO} eq "SHOW FULL PROCESSLIST";
        next
            if !defined $hashref->{STATE};

        $row_ref = $hashref;
    }

    return
        unless $row_ref;
    return $row_ref;
}

sub fetch {
    my $self = shift;

    if ( $self->has_mysql_processlist_table ) {
        return $self->fetch_table;
    }
    else {
        return $self->fetch_command;
    }
}

sub finish { shift->{sth}->finish }

1;
use strict;
use warnings;
package MySQL::Processlist::Locker::Stream::Stdin;

our $VERSION = "0.000";

sub new { bless { }, shift }

sub prepare {
    my $self    = shift;
    my $handler = shift || *ARGV;
    $self->{handler} = $handler;
}

sub execute {
    my( $self, $_time_threshold ) = @_;

    my $handler = $self->{handler};
    chomp( my $chunk = <$handler> );

    if ( defined $chunk ) {
        my $logs_ref = eval $chunk;
        $self->{logs} = $logs_ref;
    }
    else {
        $self->{logs} = [ ];
    }
}

sub fetch {
    my $self = shift;
    return shift @{ $self->{logs} };
}

1;
use strict;
use warnings;
package MySQL::Processlist::Locker::Filter;
use Data::Dumper;
use Time::Piece ( );

our $VERSION = "0.000";

my @SUSPECTED;

sub new {
    my $class = shift;
    my %param = @_;
    $param{datetime} = Time::Piece->localtime->datetime;
    return bless \%param, $class;
}

sub lock_count { shift->{locks} }

sub lock_threshold { shift->{lock_threshold} }

sub seconds_elapsed { shift->{TIME} }

sub time_threshold { shift->{time_threshold} }

sub is_candidate {
    my $self = shift;
    my $is_candidate = $self->lock_count > $self->lock_threshold
    && $self->seconds_elapsed > $self->time_threshold;
    return $is_candidate;
}

sub unbless {
    my $self = shift;
    return { %{ $self } };
}

sub abstract {
    my $self = shift;
    return join "\t", @{ $self }{ qw( datetime STATE TIME INFO ) };
}

sub DESTROY {
    my $self = shift;
    return
        if !$self->is_candidate;
    push @SUSPECTED, $self->unbless;
}

sub flush {
    ( my( @suspected ), @SUSPECTED ) = ( @SUSPECTED );
    return @suspected;
}

1;
use strict;
use warnings;
package MySQL::Processlist::Locker;
use Data::Dumper;
use Readonly;
use List::Util qw( max );


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
        print Data::Dumper->new( [ $locker_ref ] )->Terse( 1 )->Sortkeys( 1 )->Useqq( 1 )->Indent( 0 )->Dump, "\n";
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


package main;
our $interval       ||= 2;
our $iteration      ||= 5;
our $time_threshold ||= 2;
our $lock_threshold ||= 10;

my $locker = MySQL::Processlist::Locker->new(
    interval       => $interval,
    iteration      => $iteration,
    time_threshold => $time_threshold,
    lock_threshold => $lock_threshold,
    stream         => "dbi",
);
$locker->loop;

