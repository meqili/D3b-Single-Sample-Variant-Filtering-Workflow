#!/usr/bin/perl -w

use strict;

die "Usage: $0 <gene list> <VWB result>\n" unless (@ARGV == 2);

my %List_symbol = ();
my %List_ncbi = ();
my %List_ensembl = ();
open(G, $ARGV[0]) || die "$!";
while (<G>) {
	s/[\n\r]+$//;
	my @t = split(/\t/);
	$List_symbol{$t[0]} = $_;
	$List_ncbi{$t[1]} = $_ if ($t[1] ne "-");
	$List_ensembl{$t[2]} = $_ if ($t[2] ne "-");
}
close(G);

my $maf = 0.0001;
my ($max_gtar_i, $flag_keep_i, $PredCountRatio_D2T_i, $CSQ_SYMBOL_i, $CSQ_Gene_i, $entrez_gene_id_i) = ("", "", "", "", "", "");
open(V, $ARGV[1]) || die "$!";
while (<V>) {
	s/[\n\r]+$//;
	my @t = split(/\t/);
	my $gene_info = "";
	if (/^\#?chromosome/) {
		for (my $i = 0; $i < @t; $i ++) {
			$max_gtar_i = $i if ($t[$i] eq "max_gtar");
			$flag_keep_i = $i if ($t[$i] eq "flag_keep");
			$PredCountRatio_D2T_i = $i if ($t[$i] eq "PredCountRatio_D2T");
			$CSQ_SYMBOL_i = $i if ($t[$i] eq "CSQ_SYMBOL");
			$CSQ_Gene_i = $i if ($t[$i] eq "CSQ_Gene");
			$entrez_gene_id_i = $i if ($t[$i] eq "entrez_gene_id");
		}
		$gene_info = "Germline_GOIs_names\tentrez\tensembl\tgene_desc";
	} else {
		next unless ($t[$flag_keep_i] == 1 
			&& ($t[$max_gtar_i] eq "" || $t[$max_gtar_i] < $maf)
		       	&& (!defined $t[$PredCountRatio_D2T_i] || $t[$PredCountRatio_D2T_i] eq "" || $t[$PredCountRatio_D2T_i] >= 0.5));
		if (defined $t[$entrez_gene_id_i] && exists $List_ncbi{$t[$entrez_gene_id_i]}) {
			$gene_info = $List_ncbi{$t[$entrez_gene_id_i]};
		} elsif (exists $List_ensembl{$t[$CSQ_Gene_i]}) {
			$gene_info = $List_ensembl{$t[$CSQ_Gene_i]};
		} elsif (exists $List_symbol{$t[$CSQ_SYMBOL_i]}) {
			$gene_info = $List_symbol{$t[$CSQ_SYMBOL_i]};
		} else {
			next;
		}
	}
	print "$_\t$gene_info\n";
}
close(V);

