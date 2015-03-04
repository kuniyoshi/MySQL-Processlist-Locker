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
