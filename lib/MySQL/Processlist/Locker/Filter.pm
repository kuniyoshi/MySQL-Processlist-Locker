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
