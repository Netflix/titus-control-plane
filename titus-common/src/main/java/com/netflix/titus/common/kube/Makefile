.PHONY: all
all: Annotations.java

Annotations.java: annotations.podcommon go2java.sh
	./go2java.sh

annotations.podcommon:
	curl -o $@ https://raw.githubusercontent.com/Netflix/titus-kube-common/master/pod/annotations.go

.PHONY: clean
clean:
	rm Annotations.java annotations.podcommon
