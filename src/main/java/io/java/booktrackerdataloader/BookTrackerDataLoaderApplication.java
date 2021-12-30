package io.java.booktrackerdataloader;

import io.java.booktrackerdataloader.author.Author;
import io.java.booktrackerdataloader.author.AuthorRepository;
import io.java.booktrackerdataloader.book.Book;
import io.java.booktrackerdataloader.book.BookRepository;
import io.java.booktrackerdataloader.connection.DataStaxAstraProperties;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.autoconfigure.web.format.DateTimeFormatters;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
public class BookTrackerDataLoaderApplication {

    @Autowired
    AuthorRepository authorRepository;

    @Autowired
    BookRepository bookRepository;

    @Value("${datadump.location.author}")
    private String authorDumpLocation;


    @Value("${datadump.location.works}")
    private String worksDumpLocation;

    public static void main(String[] args) {
        SpringApplication.run(BookTrackerDataLoaderApplication.class, args);


    }

    private void initAuthors(){
        List<Author> authorList = new ArrayList<>();
        Path path = Paths.get(authorDumpLocation);

        try{
            Stream<String> lines = Files.lines(path);
            lines.forEach(line -> {

                //Read and Parse the Line
                String jsonString = line.substring(line.indexOf("{"));

                try {
                    JSONObject jsonObject = new JSONObject(jsonString);

                    //Construct Author Object
                    Author author = new Author();
                    author.setName(jsonObject.optString("name"));
                    author.setPersonalName(jsonObject.optString("personal_name"));
                    author.setId(jsonObject.optString("key").replace("/authors/",""));

                    //add to authorList
                    authorList.add(author);

                } catch (JSONException e) {
                    e.printStackTrace();
                }
            });

            authorRepository.saveAll(authorList);

        }catch (IOException e){
            e.printStackTrace();
        }
    }

    private void initWorks(){
        List<Book> bookList = new ArrayList<>();
        Path path = Paths.get(worksDumpLocation);
        //DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
        //DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        try{
            Stream<String> lines = Files.lines(path);
            lines.forEach(line ->{
                try {
                    String jsonString = line.substring(line.indexOf("{"));

                    JSONObject stringJson = new JSONObject(jsonString);

                    Book book = new Book();

                    book.setId(stringJson.optString("key").replace("/works/",""));
                    book.setName(stringJson.optString("title"));

                    JSONObject descJson = stringJson.optJSONObject("description");
                    if (descJson != null){
                    book.setDescription(descJson.optString("value"));
                    }

                    JSONObject dateJson = stringJson.optJSONObject("created");
                    if (dateJson != null){
                        String date = dateJson.getString("value");
                        //System.out.println(LocalDate.parse(date,formatter).toString());
                        book.setPublishedDate(LocalDate.now());
                    }

                    JSONArray coversJson = stringJson.optJSONArray("covers");
                    if (coversJson != null){
                        List<String> coverIds = new ArrayList<>();
                        for(int i = 0; i < coversJson.length(); i++){
                            coverIds.add(coversJson.getString(i));
                        }
                        book.setCoverIds(coverIds);
                    }

                    JSONArray authorsJson = stringJson.optJSONArray("authors");
                    if (authorsJson != null){
                        List<String> authorsIds = new ArrayList<>();
                        for(int i = 0; i < authorsJson.length(); i++){
                            String authorId = authorsJson.getJSONObject(i).getJSONObject("author").getString("key")
                                            .replace("/authors", "");
                            authorsIds.add(authorId);
                        }
                        book.setAuthorIds(authorsIds);

                        List<String> authorNames = authorsIds.stream().map(id -> authorRepository.findById(id))
                                .map(optionalAuthor ->{
                                    if (optionalAuthor.isEmpty()) return "Unknown Author";
                                    return optionalAuthor.get().getName();
                                }).collect(Collectors.toList());

                        book.setAuthorNames(authorNames);
                    }

                    bookList.add(book);

                } catch (JSONException e) {
                    e.printStackTrace();
                }
            });
            bookRepository.saveAll(bookList);
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    @PostConstruct
    public void start(){
        initAuthors();
        initWorks();
    }

    @Bean
    public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties astraProperties){
        Path bundle = astraProperties.getSecureConnectBundle().toPath();
        return cqlSessionBuilder -> cqlSessionBuilder.withCloudSecureConnectBundle(bundle);
    }

}
