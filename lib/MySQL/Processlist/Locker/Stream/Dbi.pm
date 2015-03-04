use strict;
use warnings;
package MySQL::Processlist::Stream::Dbi;
use DBI;
use Carp ( );

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

sub prepare {
    my $self = shift;
    my $sth = $self->{dbh}->prepare( <<END_SQL );
SELECT *
FROM PROCESSLIST
WHERE COMMAND != ? AND COMMAND != ? AND TIME > ?
END_SQL
    $self->{sth} = $sth;
}

sub execute {
    my $self           = shift;
    my $time_threshold = shift
        or Carp::croak( "time_threshold required." );
    $self->{sth}->execute( "Sleep", "Connect", $time_threshold );
}

sub fetch {
    my $self = shift;
    return $self->{sth}->fetchrow_hashref;
}

sub finish { shift->{sth}->finish }

1;
