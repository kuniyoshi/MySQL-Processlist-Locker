#!/usr/bin/perl
use 5.10.0;
use utf8;
use strict;
use warnings;
use open qw( :std :utf8 );
use autodie qw( open close );
use Data::Dumper;
use File::Spec;

$Data::Dumper::Terse    = 1;
$Data::Dumper::Sortkeys = 1;
$Data::Dumper::Indent   = 1;

our $VERSION = "0.000";

chomp( my @files = `find lib -type f -name \\*.pm` );

my $glued_code;

for my $file ( sort { scalar( File::Spec->splitdir( $b ) ) <=> scalar( File::Spec->splitdir( $a ) ) } @files ) {
    open my $FH, "<", $file;

    my $data = do { local $/; <$FH> };
    $data =~ s{
        ^ (?: __END__ | __DATA__ ) $
        .*
    }{}msx;

    for my $used_file ( @files ) {
        ( my $module_name = $used_file ) =~ s{lib/}{};
        $module_name =~ s{ [/] }{::}gmsx;
        $module_name =~ s{ [.]pm \z}{}msx;
        $data =~ s{
            ^ use \s \Q$module_name\E .*? $
        }{}msx;
        $data =~ s{
            ^ \s* require \s [^\n]* $
        }{}msx;
    }

    $glued_code .= $data;

    close $FH;
}

my $header = <<'END_HEADER';
#!/usr/bin/perl
use 5.10.0;
use strict;
use warnings;

END_HEADER

my $body = <<'END_BODY';
package main;
my $locker = MySQL::Processlist::Locker->new( stream => "dbi" );
$locker->loop;

END_BODY

print $header, $glued_code, "\n", $body;

exit;
