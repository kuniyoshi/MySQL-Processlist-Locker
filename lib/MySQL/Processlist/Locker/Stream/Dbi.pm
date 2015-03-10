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
