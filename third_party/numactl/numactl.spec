Name:		numactl
Summary:	Library for tuning for Non Uniform Memory Access machines
Version:	2.0.12
Release:	3%{dist}
# libnuma is LGPLv2 and GPLv2
# numactl binaries are GPLv2 only
License:	GPLv2
URL:		https://github.com/numactl/numactl
Source0:	https://github.com/numactl/numactl/releases/download/v2.0.12/numactl-2.0.12.tar.gz

BuildRequires:	libtool automake autoconf

ExcludeArch: s390 %{arm}

%description
Simple NUMA policy support. It consists of a numactl program to run
other programs with a specific NUMA policy.

%package libs
Summary: libnuma libraries
# There is a tiny bit of GPLv2 code in libnuma.c
License: LGPLv2 and GPLv2

%description libs
numactl-libs provides libnuma, a library to do allocations with
NUMA policy in applications.

%package devel
Summary: Development package for building Applications that use numa
Requires: %{name}-libs = %{version}-%{release}
License: LGPLv2 and GPLv2

%description devel
Provides development headers for numa library calls

%prep
%setup -q -n %{name}-%{version}

%build
%configure --prefix=/usr --libdir=%{_libdir}
# Using recipe to fix rpaths, from here:
# https://fedoraproject.org/wiki/RPath_Packaging_Draft#Removing_Rpath
sed -i -e 's|^hardcode_libdir_flag_spec=.*|hardcode_libdir_flag_spec=""|g' \
       -e 's|^runpath_var=LD_RUN_PATH|runpath_var=DIE_RPATH_DIE|g' libtool
make clean
make CFLAGS="$RPM_OPT_FLAGS -I."

%install
rm -rf $RPM_BUILD_ROOT

make DESTDIR=$RPM_BUILD_ROOT install

%ldconfig_scriptlets
%ldconfig_scriptlets libs

%files
%doc README.md
%{_bindir}/numactl
%{_bindir}/numademo
%{_bindir}/numastat
%{_bindir}/memhog
%{_bindir}/migspeed
%{_bindir}/migratepages
%{_mandir}/man8/*.8*
%exclude %{_mandir}/man2/*.2*

%files libs
%{_libdir}/libnuma.so.1.0.0
%{_libdir}/libnuma.so.1

%files devel
%{_libdir}/libnuma.so
%exclude %{_libdir}/libnuma.a
%exclude %{_libdir}/libnuma.la
%{_libdir}/pkgconfig/numa.pc
%{_includedir}/numa.h
%{_includedir}/numaif.h
%{_includedir}/numacompat1.h
%{_mandir}/man3/*.3*

