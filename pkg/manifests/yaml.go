package manifests

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"

	"k8s.io/apimachinery/pkg/api/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	logf "sigs.k8s.io/controller-runtime/pkg/runtime/log"
)

var log = logf.Log.WithName("manifests")

func NewYamlFile(path string, config *rest.Config) *YamlFile {
	client, _ := dynamic.NewForConfig(config)
	log.Info("Reading YAML file", "name", path)
	return &YamlFile{name: path, resources: parse(path), dynamicClient: client}
}

func (f *YamlFile) Apply(owner *v1.OwnerReference) error {
	for _, spec := range f.resources {
		c, err := client(spec, f.dynamicClient)
		if err != nil {
			return err
		}
		_, err = c.Get(spec.GetName(), v1.GetOptions{})
		if err == nil {
			continue
		}
		if !errors.IsNotFound(err) {
			return err
		}
		if !isClusterScoped(spec.GetKind()) {
			// apparently reference counting for cluster-scoped
			// resources is broken, so trust the GC only for ns-scoped
			// dependents
			spec.SetOwnerReferences([]v1.OwnerReference{*owner})
		}
		_, err = c.Create(&spec, v1.CreateOptions{})
		if err != nil {
			if errors.IsAlreadyExists(err) {
				continue
			}
			return err
		}
		log.Info("Created resource", "type", spec.GroupVersionKind(), "name", spec.GetName())
	}
	return nil
}

func (f *YamlFile) Delete() error {
	a := make([]unstructured.Unstructured, len(f.resources))
	copy(a, f.resources)
	// we want to delete in reverse order
	for left, right := 0, len(a)-1; left < right; left, right = left+1, right-1 {
		a[left], a[right] = a[right], a[left]
	}
	for _, spec := range a {
		c, err := client(spec, f.dynamicClient)
		if err != nil {
			return err
		}
		log.Info("Deleting resource", "type", spec.GroupVersionKind(), "name", spec.GetName())
		c.Delete(spec.GetName(), &v1.DeleteOptions{})
		// ignore GC race conditions triggered by owner references
	}
	return nil
}

func (f *YamlFile) ResourceNames() []string {
	var names []string
	for _, spec := range f.resources {
		names = append(names, fmt.Sprintf("%s (%s)", spec.GetName(), spec.GroupVersionKind()))
	}
	return names
}

type YamlFile struct {
	name          string
	dynamicClient dynamic.Interface
	resources     []unstructured.Unstructured
}

func parse(filename string) []unstructured.Unstructured {
	in, out := make(chan []byte, 10), make(chan unstructured.Unstructured, 10)
	go read(filename, in)
	go decode(in, out)
	result := []unstructured.Unstructured{}
	for spec := range out {
		result = append(result, spec)
	}
	return result
}

func buffer(file *os.File) []byte {
	var size int64 = bytes.MinRead
	if fi, err := file.Stat(); err == nil {
		size = fi.Size()
	}
	return make([]byte, size)
}

func read(filename string, sink chan []byte) {
	file, err := os.Open(filename)
	if err != nil {
		panic(err.Error())
	}
	manifests := yaml.NewDocumentDecoder(file)
	defer manifests.Close()
	buf := buffer(file)
	for {
		size, err := manifests.Read(buf)
		if err == io.EOF {
			break
		}
		b := make([]byte, size)
		copy(b, buf)
		sink <- b
	}
	close(sink)
}

func decode(in chan []byte, out chan unstructured.Unstructured) {
	for buf := range in {
		spec := unstructured.Unstructured{}
		err := yaml.NewYAMLToJSONDecoder(bytes.NewReader(buf)).Decode(&spec)
		if err != nil {
			if err != io.EOF {
				log.Error(err, "Unable to decode YAML; ignoring")
			}
			continue
		}
		out <- spec
	}
	close(out)
}

func pluralize(kind string) string {
	ret := strings.ToLower(kind)
	switch {
	case strings.HasSuffix(ret, "s"):
		return fmt.Sprintf("%ses", ret)
	case strings.HasSuffix(ret, "policy"):
		return fmt.Sprintf("%sies", ret[:len(ret)-1])
	default:
		return fmt.Sprintf("%ss", ret)
	}
}

func client(spec unstructured.Unstructured, dc dynamic.Interface) (dynamic.ResourceInterface, error) {
	groupVersion, err := schema.ParseGroupVersion(spec.GetAPIVersion())
	if err != nil {
		return nil, err
	}
	groupVersionResource := groupVersion.WithResource(pluralize(spec.GetKind()))
	if ns := spec.GetNamespace(); ns == "" {
		return dc.Resource(groupVersionResource), nil
	} else {
		return dc.Resource(groupVersionResource).Namespace(ns), nil
	}
}

func isClusterScoped(kind string) bool {
	switch strings.ToLower(kind) {
	case "namespace", "clusterrole", "clusterrolebinding", "customresourcedefinition":
		return true
	}
	return false
}
