#!/usr/bin/perl -w
use strict;
my ($pick_i, $symbol_i, $consequence_i, $info_i) = ("", "", "", "");

while (<>) {
	s/[\n\r]+$//;
	if (/^\#\#/) { # info field header for CSQ
		print STDOUT "$_\n";
		if (/^\#\#INFO=\<ID=CSQ,.+Format:\s+([^\"]+)\"\>$/) {
			my @csq_fields = split(/\|/, $1);
			for (my $i = 0; $i < @csq_fields; $i ++) {
				$pick_i = $i if ($csq_fields[$i] =~/^pick$/i);
				$symbol_i = $i if ($csq_fields[$i] =~/^symbol$/i);
				$consequence_i = $i if ($csq_fields[$i] =~/^consequence$/i);
			}
		}
	} else {
		my @t = split(/\t/);
		if (/^\#CHR/) { # header line
			for (my $i = 0; $i < @t; $i ++) {
				if ($t[$i] eq "INFO") {
					$info_i = $i;
					last;
				}
			}
		} elsif ($t[$info_i] =~ /CSQ=([^;]+)/) {
			my $ANN = $1;
			my @vep = split(/,/, $ANN);
			my $gene = "";
			my $consequence = "";
			for (my $i = 0; $i < @vep; $i ++) {
				my @transcripts = split(/\|/, $vep[$i]);
				if (defined $transcripts[$pick_i] && $transcripts[$pick_i] eq "1") {
					$gene = $transcripts[$symbol_i];
					$consequence = $transcripts[$consequence_i];
				}
			}
			$t[$info_i] = "Gene.refGene=$gene;Func.refGene=$consequence;$t[$info_i]" if ($gene ne "");
		}
		print STDERR join("\t", @t), "\n";
	}
}
