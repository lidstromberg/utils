package utils

import (
	"bufio"
	"context"
	"io"
	"os"
	"sync"

	"github.com/linkedin/goavro"
)

//FromAvroFile sends back a map of content from an avro file
func FromAvroFile(ctx context.Context, filename string, bufferSize int) <-chan interface{} {
	//waitgroup to control goroutines
	var wg sync.WaitGroup

	//make the buffered channel
	result := make(chan interface{}, bufferSize)

	//function to process file
	fn := func(filename string) {
		fileName := filename

		defer wg.Done()

		//attempt to open the avro file
		fl, err := os.Open(fileName)
		if err != nil {
			result <- err
			return
		}
		defer fl.Close()

		//create a buffered reader over the file
		rdr := bufio.NewReader(fl)

		//and make an OCF reader from the buffered reader
		ocfrdr, err := goavro.NewOCFReader(rdr)
		if err != nil {
			result <- err
			return
		}

		//scan each datum in the file
		for ocfrdr.Scan() {
			dt, err := ocfrdr.Read()

			//report each error
			if err != nil {
				result <- err
				return
			}

			//attempt the convert the datum back to a map
			mp, ok := dt.(map[string]interface{})
			if !ok {
				result <- err
				return
			}

			//send the data on the channel or quit if ctx.Done
			select {
			case <-ctx.Done():
				return
			default:
				result <- mp
			}
		}

		if ocfrdr.Err() != nil {
			result <- ocfrdr.Err()
		}
	}

	wg.Add(1)
	go fn(filename)

	go func() {
		wg.Wait()
		close(result)
	}()

	return result
}

//ToAvroFile writes data from a channel into an Avro file
func ToAvroFile(ctx context.Context, filename, compressiontype string, codec *goavro.Codec, data <-chan interface{}) error {

	//attempt to create the file for writing
	flout, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer func(ioc io.Closer) error {
		if err := ioc.Close(); err != nil {
			return err
		}

		return nil
	}(flout)

	//create the writer config
	cfg := goavro.OCFConfig{
		W:               flout,
		Codec:           codec,
		CompressionName: compressiontype,
	}

	//create the OCF Writer
	fwr, err := goavro.NewOCFWriter(cfg)
	if err != nil {
		return err
	}

	//create the attribute map
	at := make(map[string]interface{})

	//assume the channel state is open
	ischopen := true

	for ischopen {
		select {
		//if the context Done has been received, then reset ischopen and break out of the loop..
		case <-ctx.Done():
			{
				ischopen = false
				break
			}
			//or.. read from the channel
		case r, ok := <-data:
			//..if is not ok, then the channel has been closed by the producer, so reset ischopen and break..
			if ok == false {
				ischopen = false
				break
			}
			//..if is ok and is castable to error, then it's an error.. reset ischopen and test log as fatal
			if _, ok := r.(error); ok {
				err := r.(error)
				ischopen = false
				return err
			}
			//..if is ok and is castable to map string interface
			if _, ok := r.(map[string]interface{}); ok {
				at = r.(map[string]interface{})

				_, err = codec.BinaryFromNative(nil, at)
				if err != nil {
					return err
				}

				err = fwr.Append([]interface{}{at})
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}
