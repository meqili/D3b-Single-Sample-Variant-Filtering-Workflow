#!/usr/bin/perl -w

# version: 20231007
use strict;

die "Usage: $0 <header> <VEP vcf>" unless (@ARGV == 2);

my @info_fields = ();
my @format_fields = ();
my @csq_fields = ();
my $PICK_i = "";

open(H, $ARGV[0]) || die "$!";
while (<H>) {
	s/[\n\r]+$//;
	if (/^##INFO=\<ID=([^=,]+),/) {
		my $info = $1;
		if ($info eq "CSQ") {
			if (/Description=\"[^"=]+Format:\s+(.+)\"\>$/) {
				@csq_fields = split(/\|/, $1);
				for (my $i = 0; $i < @csq_fields; $i ++) {
					$PICK_i = $i if ($csq_fields[$i] eq "PICK");
				}
			}
		} else {
			push(@info_fields, $info);
		}
	} elsif (/^##FORMAT=\<ID=([^=,]+),/) {
		push(@format_fields, $1);
	}
}
close(H);

# print "$PICK_i\n";
my $extra_last = 0; # one extra column added to the last of a typical vcf file

open(V, $ARGV[1]) || die "$!";
while (<V>) {
	s/[\n\r]+$//;
	my @t = split(/\t/);
	if (/^\#?CHR/i) {
		$extra_last = 1 if ($t[-1] =~/inheritance|parent/i);
		print join("\t", @t[0 .. 6]), "\t", join("\t", map {"INFO_$_"} @info_fields), "\t", join("\t", map {"CSQ_$_"} @csq_fields);
		for (my $i = 9; $i < scalar(@t) - ($extra_last == 1 ? 1 : 0); $i ++) {
			print "\t", join("\t", map {"$t[$i]_FORMAT_$_"} @format_fields), "\t$t[$i]_AdjustedGenotype";
		}
		print $extra_last == 1 ? "\t$t[-1]\n" : "\n";
	} elsif (/^[^#]/) {
		next if ($t[4] eq "*"); # skip variants with "*" as alternate allele
		my $output = join("\t", @t[0 .. 6]);
		for (my $i = 0; $i < @info_fields; $i ++) {
			$output .= $t[7] =~/$info_fields[$i]=([^;]+);/ ? ($1 ne "." ? "\t$1" : "\t-") : "\t-";
		}
		if (/;?CSQ=([^;\s]+);?/) { # there could be equal signs "=" in annotations for synonymous variants, so don't use pattern /;?CSQ=([^=;\s]+);?/
			my $picked = 0;
			my @transcripts = split(/,/, $1);
			for (my $i = 0; $i < @transcripts; $i ++) {
				# print "$transcripts[$i]\n";
				my @csq_transcript = split(/\|/, $transcripts[$i], -1);
				# print "$csq_transcript[$PICK_i]\n";
				if ($csq_transcript[$PICK_i] eq "1") {
					for (my $i = 0; $i < @csq_transcript; $i ++) {
						$output .= ($csq_transcript[$i] eq "" ? "\t-" : "\t$csq_transcript[$i]");
					}
					# $output .= "\t" . join("\t", @csq_transcript);
					$picked = 1;
					last;
				}
			}
		} else { # just in case no CSQ annotation is included in a variant
			$output .= "\t-" x scalar(@csq_fields);
		}
		my %format_hash = ();
		my @format_in_sample = split(/:/, $t[8]);
		for (my $index = 0; $index < @format_in_sample; $index ++) {
			$format_hash{$format_in_sample[$index]} = $index;
		}
		my @format_indices = ();
		for (my $i = 0; $i < @format_fields; $i ++) {
			if ($t[8] =~/:?$format_fields[$i]:?/) {
				push(@format_indices, $format_hash{$format_fields[$i]});
			} else {
				push(@format_indices, "-");
			}
		}
		for (my $sample = 9; $sample < scalar(@t) - $extra_last; $sample ++) {
			my @sample_details = split(/:/, $t[$sample]);
			for (my $i = 0; $i < @format_indices; $i ++) {
				$output .= "\t" . ($format_indices[$i] eq "-" ? "-" : $sample_details[$format_indices[$i]]);
			}
			$output .= "\t" . SeqQC_ignore_GT($t[8], $t[$sample]);
		}
		if (scalar(@t) > 10) { # multi-sample vcf, print every line
			print $output . ($extra_last == 1 ? "\t$t[-1]\n" : "\n");
		} else { # single-sample vcf, print only variant line (those with genotype ./. or 0/0 are removed)
			print "$output\n" if (SeqQC_ignore_GT($t[8], $t[9]) ne "./." && SeqQC_ignore_GT($t[8], $t[9]) ne "0/0");
		}
	}
}
close(V);

sub SeqQC_ignore_GT {
	my ($format, $seq_details) = @_;
	my @format_array = split(/:/, $format);
	my ($GT_i, $AD_i, $DP_i) = ("", "", "");
	for (my $i = 0; $i < @format_array; $i ++) {
		$GT_i = $i if ($format_array[$i] eq "GT");
		$AD_i = $i if ($format_array[$i] eq "AD");
		$DP_i = $i if ($format_array[$i] eq "DP");
	}
	my @seq_details_array = split(/:/, $seq_details);
	my $return_value = $seq_details_array[$GT_i];
	my $allele_balance_threshold = 0.8; # see comments below
	if ($seq_details eq "." || $seq_details_array[$DP_i] eq ".") {
		$return_value = "./.";
	} else {
		my @allele_depths = split(/,/, $seq_details_array[$AD_i]);
		if (scalar(@allele_depths) == 2) {
			my $allele_depth_sum = $seq_details_array[$AD_i];
			$allele_depth_sum =~s/,/\+/g;
			$allele_depth_sum = eval($allele_depth_sum);
			if ($allele_depth_sum == 0) { # no qualified sequencing depth
				$return_value = "./.";
			} else { # there is even 7,0 being called 1/1 so I decided to rewrite this part completely ignoring the original GT field
				# if ($seq_details_array[$GT_i] eq "0/0") { # genotype call of wildtype/homozygous for reference allele
				# 	if ($allele_depths[0] / $allele_depth_sum <= $allele_balance_threshold) { # depth of reference allele is <= 4/5 of total allele, e.g. 80,20
				# 		$return_value = "0/1";
				# 	}
				# } elsif ($seq_details_array[$GT_i] eq "0/1") { # genotype call of heterozygote
				# 	if ($allele_depths[1] / $allele_depth_sum < 1 - $allele_balance_threshold) { # depth of alternate allele is < 1/5 of total depth, e.g. 81,19
				# 		$return_value = "0/0";
				# 	} elsif ($allele_depths[1] / $allele_depth_sum >= $allele_balance_threshold) { # depth of alternate allele is >= 4/5 of total depth, e.g. 19,81
				# 		$return_value = "1/1";
				# 	}
				# }
				if ($allele_depths[1] / $allele_depth_sum >= $allele_balance_threshold) {
					$return_value = "1/1";
				} elsif ($allele_depths[1] / $allele_depth_sum < $allele_balance_threshold && $allele_depths[1] / $allele_depth_sum >= 1 - $allele_balance_threshold) {
					$return_value = "0/1";
				} elsif ($allele_depths[1] / $allele_depth_sum < 1 - $allele_balance_threshold) {
					$return_value = "0/0";
				}
			}
		}
	}
	return $return_value;
}
