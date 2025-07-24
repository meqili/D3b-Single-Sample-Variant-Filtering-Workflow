#!/usr/bin/perl -w
use strict;
my ($pick_i, $symbol_i, $consequence_i, $info_i, $filter_i, $format_i) = ("", "", "", "", "", "");

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
				$filter_i = $i if ($t[$i] eq "FILTER");
				$info_i = $i if ($t[$i] eq "INFO");
				$format_i = $i if ($t[$i] eq "FORMAT");
			}
		} elsif ($t[$info_i] =~ /CSQ=([^;]+)/) {
			# FILTER == PASS
			next unless ($t[$filter_i] eq "PASS");
			# total sequencing depth (DP) >= 10 and genotype quality (GQ) >= 20 in FORMAT
			my ($dp_i, $gq_i) = ("", "");
			my @format_item = split(/:/, $t[$format_i]);
			for (my $i = 0; $i < @format_item; $i ++) {
				$dp_i = $i if ($format_item[$i] eq "DP");
				$gq_i = $i if ($format_item[$i] eq "GQ");
			}
			my @format_value = (split(/:/, $t[-1]));
			next unless ($format_value[$dp_i] >= 10 && $format_value[$gq_i] >= 20);
			# INFO_QD >= 2.0, INFO_FS <= 60.0, INFO_MQ >= 40.0, INFO_MQRankSum > -12.5, INFO_ReadPosRankSum > -8.0 and INFO_SOR <= 3.0
			next if ($t[$info_i] =~/(?:^|;)QD=([^=;]+)(?:;|$)/ && $1 < 2.0);
			next if ($t[$info_i] =~/(?:^|;)FS=([^=;]+)(?:;|$)/ && $1 > 60.0);
			next if ($t[$info_i] =~/(?:^|;)MQ=([^=;]+)(?:;|$)/ && $1 < 40.0);
			next if ($t[$info_i] =~/(?:^|;)MQRankSum=([^=;]+)(?:;|$)/ && $1 <= -12.5);
			next if ($t[$info_i] =~/(?:^|;)ReadPosRankSum=([^=;]+)(?:;|$)/ && $1 <= -8.0);
			next if ($t[$info_i] =~/(?:^|;)SOR=([^=;]+)(?:;|$)/ && $1 > 3.0);
			# Annovar-like gene annotation
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
